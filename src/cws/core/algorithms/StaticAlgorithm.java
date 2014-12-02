package cws.core.algorithms;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeMap;

import cws.core.Cloud;
import cws.core.EnsembleManager;
import cws.core.Provisioner;
import cws.core.Scheduler;
import cws.core.VM;
import cws.core.VMFactory;
import cws.core.VMListener;
import cws.core.WorkflowEngine;
import cws.core.WorkflowEvent;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.core.VMType;
import cws.core.dag.DAG;
import cws.core.dag.DAGJob;
import cws.core.dag.Task;
import cws.core.dag.algorithms.CriticalPath;
import cws.core.dag.algorithms.TopologicalOrder;
import cws.core.engine.Environment;
import cws.core.jobs.Job;
import cws.core.jobs.Job.Result;
import cws.core.jobs.JobListener;

public abstract class StaticAlgorithm extends HomogeneousAlgorithm implements Provisioner, Scheduler, VMListener, JobListener {
    /** Plan */
    private Plan plan = new Plan();

    /** List of DAGs that were admitted to run */
    private final List<DAG> admittedDAGs = new LinkedList<DAG>();

    /** Set of jobs that are ready to run arranged by Task */
    private final HashMap<Task, Job> readyJobs = new HashMap<Task, Job>();

    /** Mapping of Task to the VM that will run the task */
    private final HashMap<Task, VM> taskMap = new HashMap<Task, VM>();

    /** Schedule of tasks for each VM */
    private final HashMap<VM, LinkedList<Task>> vmQueues = new HashMap<VM, LinkedList<Task>>();

    /** Set of idle VMs */
    private final HashSet<VM> idleVms = new HashSet<VM>();

    private long planningStartWallTime;
    private long planningFinishWallTime;

    public StaticAlgorithm(double budget, double deadline, List<DAG> dags, AlgorithmStatistics ensembleStatistics,
            Environment environment, CloudSimWrapper cloudsim) {
        super(budget, deadline, dags, ensembleStatistics, environment, cloudsim);
    }

    @Override
    public long getPlanningnWallTime() {
        return planningFinishWallTime - planningStartWallTime;
    }

    /**
     * Develop a plan for running as many DAGs as we can
     */
    public void plan() {
        // We assume the dags are in priority order
        for (DAG dag : getAllDags()) {
            try {
                Plan newPlan = planDAG(dag, plan);
                // Plan was feasible
                if (newPlan.getCost() <= getBudget()) {
                    admittedDAGs.add(dag);
                    plan = newPlan;
                    getCloudsim().log("Admitting DAG. Cost of new plan: " + plan.getCost());
                } else {
                    getCloudsim().log("Rejecting DAG: New plan exceeds budget: " + newPlan.getCost());
                }
            } catch (NoFeasiblePlan m) {
                getCloudsim().log("Rejecting DAG: " + m.getMessage());
            }
        }

        for (Resource r : plan.resources) {
            // create VM
            VMType vmType = getEnvironment().getVMType();
            VM vm = VMFactory.createVM(vmType, getCloudsim());

            // Build task<->vm mappings
            LinkedList<Task> vmQueue = new LinkedList<Task>();
            vmQueues.put(vm, vmQueue);
            for (Double start : r.schedule.navigableKeySet()) {
                Slot slot = r.schedule.get(start);
                Task task = slot.task;
                taskMap.put(task, vm);
                vmQueue.add(task);
            }

            // Launch the VM at its appointed time
            launchVM(vm, r.getStart());

        }

        // Submit admitted DAGs
        for (DAG dag : admittedDAGs) {
            submitDAG(dag);
        }
    }

    /**
     * Develop a plan for a single DAG
     */
    abstract Plan planDAG(DAG dag, Plan currentPlan) throws NoFeasiblePlan;

    /**
     * Assign deadlines to each task in the DAG
     */
    protected HashMap<Task, Double> getDeadlineDistribution(TopologicalOrder order, HashMap<Task, Double> runtimes,
            double alpha) {
        // Sanity check
        if (alpha < 0 || alpha > 1) {
            throw new RuntimeException("Invalid alpha: " + alpha + ". Valid range is [0,1].");
        }

        // The level of each task is max[p in parents](p.level) + 1
        HashMap<Task, Integer> levels = new HashMap<Task, Integer>();
        int numlevels = 0;
        for (Task t : order) {
            int level = 0;
            for (Task p : t.getParents()) {
                int plevel = levels.get(p);
                level = Math.max(level, plevel + 1);
            }
            levels.put(t, level);
            numlevels = Math.max(numlevels, level + 1);
        }

        /*
         * Compute:
         * 1. Total number of tasks in DAG
         * 2. Total number of tasks in each level
         * 3. Total runtime of tasks in DAG
         * 4. Total runtime of tasks in each level
         */
        double totalTasks = 0;
        double[] totalTasksByLevel = new double[numlevels];

        double totalRuntime = 0;
        double[] totalRuntimesByLevel = new double[numlevels];

        for (Task task : order) {
            double runtime = runtimes.get(task);
            int level = levels.get(task);

            totalRuntime += runtime;
            totalRuntimesByLevel[level] += runtime;

            totalTasks += 1;
            totalTasksByLevel[level] += 1;
        }

        /*
         * The excess time share for each level is:
         * 
         * // tasksInLevel \ / runtimeInLevel \\
         * frac = || alpha * ------------ | + | (1-alpha) * -------------- ||
         * \\ totalTasks / \ totalRuntime //
         * 
         * share = frac * (deadline - critical_path)
         * 
         * In other words, each level gets a fraction of the spare time that is
         * proportional to the combination of the number of tasks it has as well
         * as the total runtime of those tasks.
         */
        double[] shares = new double[numlevels];
        CriticalPath path = newCriticalPath(order, runtimes);
        double criticalPathLength = path.getCriticalPathLength();
        double spare = getDeadline() - criticalPathLength;
        // subtract estimates for provisioning and deprovisioning delays
        spare = spare - getEnvironment().getVMProvisioningOverallDelayEstimation();
        for (int i = 0; i < numlevels; i++) {

            double taskPart = alpha * (totalTasksByLevel[i] / totalTasks);
            double runtimePart = (1 - alpha) * (totalRuntimesByLevel[i] / totalRuntime);

            shares[i] = (taskPart + runtimePart) * spare;
        }

        /*
         * The deadline of a task t is:
         * 
         * t.deadline = max[p in t.parents](p.deadline) + t.runtime + shares[t.level]
         */
        HashMap<Task, Double> deadlines = new HashMap<Task, Double>();
        for (Task task : order) {
            int level = levels.get(task);
            double latestDeadline = 0.0;
            for (Task parent : task.getParents()) {
                double pdeadline = deadlines.get(parent);
                latestDeadline = Math.max(latestDeadline, pdeadline);
            }
            double runtime = runtimes.get(task);
            double deadline = latestDeadline + runtime + shares[level];
            deadlines.put(task, deadline);
        }

        return deadlines;
    }

    private void submitDAG(DAG dag) {
        List<DAG> dags = getAllDags();
        int priority = dags.indexOf(dag);
        DAGJob dagJob = new DAGJob(dag, getEnsembleManager().getId());
        dagJob.setPriority(priority);
        getCloudsim().send(getEnsembleManager().getId(), getWorkflowEngine().getId(), 0.0, WorkflowEvent.DAG_SUBMIT,
                dagJob);
    }

    @Override
    public void provisionResources(WorkflowEngine engine) {
        // Do nothing
    }

    @Override
    public void scheduleJobs(WorkflowEngine engine) {
        // Just clear any jobs that were queued
        engine.getQueuedJobs().clear();
    }

    @Override
    public void vmLaunched(VM vm) {
        idleVms.add(vm);
        submitNextTaskFor(vm);
    }

    @Override
    public void vmTerminated(VM vm) {
        idleVms.remove(vm);
    }

    @Override
    public void jobReleased(Job job) {
        Task task = job.getTask();

        // Mark the job ready
        readyJobs.put(task, job);

        // Try to submit the next task
        VM vm = taskMap.get(task);
        submitNextTaskFor(vm);
    }

    @Override
    public void jobSubmitted(Job job) {
    }

    @Override
    public void jobStarted(Job job) {
    }

    @Override
    public void jobFinished(Job job) {
        VM vm = job.getVM();

        // Sanity check
        DAG dag = job.getDAGJob().getDAG();
        if (!admittedDAGs.contains(dag)) {
            throw new RuntimeException("Running DAG that wasn't accepted");
        }

        // If the task failed, retry it on the same VM
        if (job.getResult() == Result.FAILURE) {
            // We need to re-add the task to the VM's queue here.
            // The workflow engine will take care of releasing a new Job
            // for the task, we just have to be ready for it when the next
            // task for this VM is submitted at the end of this method.
            LinkedList<Task> queue = vmQueues.get(vm);
            queue.addFirst(job.getTask());
        }

        idleVms.add(vm);
        submitNextTaskFor(vm);
    }

    private void submitNextTaskFor(VM vm) {
        // If the VM is busy, do nothing
        if (!idleVms.contains(vm)) {
            return;
        }

        LinkedList<Task> vmqueue = vmQueues.get(vm);
        // Get next task for VM
        Task task = vmqueue.peek();
        if (task == null) {
            // No more tasks
            getCloud().terminateVM(vm);
        } else {
            // If job for task is ready
            if (readyJobs.containsKey(task)) {
                // Submit job
                Job next = readyJobs.get(task);
                submitJob(vm, next);
            }
        }
    }

    private void launchVM(VM vm, double start) {
        double now = getCloudsim().clock();
        double delay = start - now;
        getCloudsim().send(getWorkflowEngine().getId(), getCloud().getId(), delay, WorkflowEvent.VM_LAUNCH, vm);
    }

    private void submitJob(VM vm, Job job) {
        Task task = job.getTask();

        // Advance queue
        LinkedList<Task> vmqueue = vmQueues.get(vm);
        Task next = vmqueue.poll();
        if (next != task) {
            throw new RuntimeException("Not next task");
        }

        // Remove the job from the ready queue
        if (!readyJobs.containsKey(task)) {
            throw new RuntimeException("Task not ready");
        }
        readyJobs.remove(task);

        // Submit the job to the VM
        idleVms.remove(vm);
        job.setVM(vm);
        vm.jobSubmit(job);
    }

    @Override
    public void simulateInternal() {
        prepareEnvironment();

        planningStartWallTime = System.nanoTime();

        plan();

        planningFinishWallTime = System.nanoTime();

        getCloudsim().startSimulation();
    }

    private void prepareEnvironment() {
        Cloud cloud = new Cloud(getCloudsim());
        WorkflowEngine engine = new WorkflowEngine(this, this, getBudget(), getDeadline(), getCloudsim());
        EnsembleManager manager = new EnsembleManager(engine, getCloudsim());

        setCloud(cloud);
        setEnsembleManager(manager);
        setWorkflowEngine(engine);
        cloud.addVMListener(this);
        engine.addJobListener(this);
    }

    /**
     * Computes and returns {@link TopologicalOrder} for the given parameters.
     * @param dag DAG with tasks
     * @param runtimes hash map of Task -> predicted runtime
     * @return TopologicalOrder
     * @throws NoFeasiblePlan when best critical path > deadline
     */
    protected TopologicalOrder computeTopologicalOrder(DAG dag, HashMap<Task, Double> runtimes) throws NoFeasiblePlan {
        TopologicalOrder order = new TopologicalOrder(dag);
        for (Task task : order) {
            double runtime = getPredictedTaskRuntime(task);
            runtimes.put(task, runtime);
        }

        // Make sure a plan is feasible given the deadline and available VMs
        // FIXME Later we will assign each task to its fastest VM type before this
        CriticalPath path = newCriticalPath(order, runtimes);
        double minimalTime = path.getCriticalPathLength() + getEnvironment().getVMProvisioningOverallDelayEstimation();
        if (minimalTime > getDeadline()) {
            throw new NoFeasiblePlan("Best critical path + provisioning estimates (" + minimalTime + ") "
                    + "> deadline (" + getDeadline() + ")");
        }
        return order;
    }

    /**
     * Creates and returns new {@link CriticalPath} object. May be overridden by subclasses to provide different
     * implementations.
     */
    protected CriticalPath newCriticalPath(TopologicalOrder order, HashMap<Task, Double> runtimes) {
        return new CriticalPath(order, runtimes, getEnvironment().getVMType());
    }

    /**
     * Estimates and returns total task runtime. May be override by subclasses to provide values based on different
     * criteria.
     */
    protected double getPredictedTaskRuntime(Task task) {
        return getEnvironment().getComputationPredictedRuntime(task);
    }

    class Slot {
        Task task;
        double start;
        double duration;

        public Slot(Task task, double start, double duration) {
            this.task = task;
            this.start = start;
            this.duration = duration;
        }
    }

    private static int nextresourceid = 0;

    class Resource {
        int id = nextresourceid++;
        Environment environment;
        TreeMap<Double, Slot> schedule;

        public Resource(Resource other) {
            this(other.environment);
            for (Double s : other.schedule.navigableKeySet()) {
                schedule.put(s, other.schedule.get(s));
            }
        }

        public Resource(Environment environment) {
            this.environment = environment;
            this.schedule = new TreeMap<Double, Slot>();
        }

        public SortedSet<Double> getStartTimes() {
            return schedule.navigableKeySet();
        }

        public double getStart() {
            if (schedule.size() == 0) {
                return 0.0;
            }
            return schedule.firstKey();
        }

        public double getEnd() {
            if (schedule.size() == 0) {
                return 0.0;
            }
            double last = schedule.lastKey();
            Slot lastSlot = schedule.get(last);
            return last + lastSlot.duration + environment.getDeprovisioningDelayEstimation();
        }

        public int getFullBillingUnits() {
            return getFullBillingUnitsWith(getStart(), getEnd());
        }

        public int getFullBillingUnitsWith(double start, double end) {
            double seconds = end - start;
            double units = seconds / environment.getBillingTimeInSeconds();
            int rounded = (int) Math.ceil(units);
            return Math.max(1, rounded);
        }

        public double getCostWith(double start, double end) {
            return environment.getVMCostFor(end - start);
        }

        public double getCost() {
            return getCostWith(getStart(), getEnd());
        }

        public double getUtilization() {
            double runtime = 0.0;
            for (Slot sl : schedule.values()) {
                runtime += sl.duration;
            }
            return runtime / (getFullBillingUnits() * environment.getBillingTimeInSeconds());
        }
    }

    class Solution {
        double cost;
        Resource resource;
        Slot slot;
        boolean newresource;

        public Solution(Resource resource, Slot slot, double cost, boolean newresource) {
            this.resource = resource;
            this.slot = slot;
            this.cost = cost;
            this.newresource = newresource;
        }

        public boolean betterThan(Solution other) {
            // A solution is better than no solution
            if (other == null) {
                return true;
            }

            // Cheaper solutions are better
            if (this.cost < other.cost) {
                return true;
            }
            if (this.cost > other.cost) {
                return false;
            }

            // Existing resources are better
            if (!this.newresource && other.newresource) {
                return true;
            }
            if (this.newresource && !other.newresource) {
                return false;
            }

            // Earlier starts are better
            if (this.slot.start < other.slot.start) {
                return true;
            }
            if (this.slot.start > other.slot.start) {
                return false;
            }

            return true;
        }

        public void addToPlan(Plan p) {
            resource.schedule.put(slot.start, slot);
            p.resources.add(resource);
        }
    }

    class Plan {
        LinkedHashSet<Resource> resources;

        public Plan() {
            this.resources = new LinkedHashSet<Resource>();
        }

        public Plan(Plan other) {
            this.resources = new LinkedHashSet<Resource>();
            for (Resource r : other.resources) {
                this.resources.add(new Resource(r));
            }
        }

        public double getCost() {
            double cost = 0.0;
            for (Resource r : resources) {
                cost += r.getCost();
            }
            return cost;
        }
    }

    class NoFeasiblePlan extends Exception {
        private static final long serialVersionUID = 1L;

        public NoFeasiblePlan(String msg) {
            super(msg);
        }
    }
}
