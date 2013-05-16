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
import cws.core.VMListener;
import cws.core.WorkflowEngine;
import cws.core.WorkflowEvent;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.dag.DAG;
import cws.core.dag.DAGJob;
import cws.core.dag.DAGJobListener;
import cws.core.dag.Task;
import cws.core.dag.algorithms.CriticalPath;
import cws.core.dag.algorithms.TopologicalOrder;
import cws.core.experiment.VMFactory;
import cws.core.jobs.Job;
import cws.core.jobs.JobListener;
import cws.core.jobs.Job.Result;
import cws.core.log.WorkflowLog;
import cws.core.storage.VoidStorageManager;

public abstract class StaticAlgorithm extends Algorithm implements Provisioner, Scheduler, VMListener, JobListener,
        DAGJobListener {

    private CloudSimWrapper cloudsim;

    /** Engine that executes workflows */
    private WorkflowEngine engine;

    /** Ensemble manager that submits DAGs */
    private EnsembleManager manager;

    /** Cloud to provision VMs from */
    private Cloud cloud;

    /** Plan */
    private Plan plan = new Plan();

    /** List of DAGs that were admitted to run */
    private List<DAG> admittedDAGs = new LinkedList<DAG>();

    /** Set of jobs that are ready to run arranged by Task */
    private HashMap<Task, Job> readyJobs = new HashMap<Task, Job>();

    /** Mapping of Task to the VM that will run the task */
    private HashMap<Task, VM> taskMap = new HashMap<Task, VM>();

    /** Schedule of tasks for each VM */
    private HashMap<VM, LinkedList<Task>> vmQueues = new HashMap<VM, LinkedList<Task>>();

    /** Set of idle VMs */
    private HashSet<VM> idle = new HashSet<VM>();

    private int dagsFinished = 0;

    protected double actualDagFinishTime = 0.0;
    protected double actualJobFinishTime = 0.0;

    protected long planningStartWallTime;
    protected long simulationStartWallTime;
    protected long simulationFinishWallTime;

    public StaticAlgorithm(double budget, double deadline, List<DAG> dags, CloudSimWrapper cloudsim) {
        super(budget, deadline, dags);
        this.cloudsim = cloudsim;
    }

    @Override
    public void setCloud(Cloud c) {
        if (cloud != null)
            cloud.removeVMListener(this);
        cloud = c;
        cloud.addVMListener(this);
    }

    @Override
    public void setWorkflowEngine(WorkflowEngine e) {
        if (engine != null)
            engine.removeJobListener(this);
        engine = e;
        engine.addJobListener(this);
    }

    public void setEnsembleManager(EnsembleManager m) {
        if (manager != null)
            manager.removeDAGJobListener(this);
        manager = m;
        manager.addDAGJobListener(this);
    }

    public Plan getPlan() {
        return plan;
    }

    public double getPlanCost() {
        return plan.getCost();
    }

    @Override
    public double getActualCost() {
        return engine.getCost();
    }

    @Override
    public List<DAG> getCompletedDAGs() {
        return this.admittedDAGs;
    }

    @Override
    public double getActualVMFinishTime() {
        double finish = 0.0;
        for (VM vm : vmQueues.keySet()) {
            finish = Math.max(finish, vm.getTerminateTime());
        }
        return finish;
    }

    @Override
    public double getActualDagFinishTime() {
        return actualDagFinishTime;
    }

    @Override
    public double getActualJobFinishTime() {
        return actualJobFinishTime;
    }

    public double getEstimatedProvisioningDelay() {
        return 0.0;
    }

    public double getEstimatedDeprovisioningDelay() {
        return 0.0;
    }

    @Override
    public long getSimulationWallTime() {
        return simulationFinishWallTime - simulationStartWallTime;
    }

    @Override
    public long getPlanningnWallTime() {
        return simulationStartWallTime - planningStartWallTime;
    }

    /**
     * @return the cloudsim
     */
    protected CloudSimWrapper getCloudsim() {
        return cloudsim;
    }

    /**
     * Develop a plan for running as many DAGs as we can
     */
    public void plan() {
        // We assume the dags are in priority order
        for (DAG dag : getDAGs()) {
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
            // Create VM
            VMType type = r.vmtype;
            VM vm = VMFactory.createVM(type.mips, 1, 1, type.price, cloudsim);

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
    HashMap<Task, Double> deadlineDistribution(TopologicalOrder order, HashMap<Task, Double> runtimes, double alpha) {

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

        for (Task t : order) {
            double runtime = runtimes.get(t);
            int level = levels.get(t);

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
        CriticalPath path = new CriticalPath(order, runtimes);
        double criticalPathLength = path.getCriticalPathLength();
        double spare = getDeadline() - criticalPathLength;
        // subtract estimates for provisioning and deprovisioning delays
        spare = spare - (getEstimatedProvisioningDelay() + getEstimatedDeprovisioningDelay());
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
        for (Task t : order) {
            int level = levels.get(t);
            double latestDeadline = 0.0;
            for (Task p : t.getParents()) {
                double pdeadline = deadlines.get(p);
                latestDeadline = Math.max(latestDeadline, pdeadline);
            }
            double runtime = runtimes.get(t);
            double deadline = latestDeadline + runtime + shares[level];
            deadlines.put(t, deadline);
        }

        return deadlines;
    }

    private void submitDAG(DAG dag) {
        List<DAG> dags = getDAGs();
        int priority = dags.indexOf(dag);
        DAGJob dagJob = new DAGJob(dag, manager.getId());
        dagJob.setPriority(priority);
        cloudsim.send(manager.getId(), engine.getId(), 0.0, WorkflowEvent.DAG_SUBMIT, dagJob);
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
        idle.add(vm);
        submitNextTaskFor(vm);
    }

    @Override
    public void vmTerminated(VM vm) {
        idle.remove(vm);
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

        // Update the finish time
        if (job.getResult() == Result.SUCCESS) {
            actualJobFinishTime = Math.max(actualJobFinishTime, job.getFinishTime());
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

        idle.add(vm);
        submitNextTaskFor(vm);
    }

    private void submitNextTaskFor(VM vm) {
        // If the VM is busy, do nothing
        if (!idle.contains(vm))
            return;

        LinkedList<Task> vmqueue = vmQueues.get(vm);

        // Get next task for VM
        Task task = vmqueue.peek();
        if (task == null) {
            // No more tasks
            terminateVM(vm);
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
        double now = cloudsim.clock();
        double delay = start - now;
        cloudsim.send(engine.getId(), cloud.getId(), delay, WorkflowEvent.VM_LAUNCH, vm);
    }

    private void terminateVM(VM vm) {
        cloudsim.send(engine.getId(), cloud.getId(), 0.0, WorkflowEvent.VM_TERMINATE, vm);
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
        idle.remove(vm);
        job.setVM(vm);
        cloudsim.send(engine.getId(), vm.getId(), 0.0, WorkflowEvent.JOB_SUBMIT, job);
    }

    @Override
    public void dagStarted(DAGJob dagJob) {
        /* Do nothing */
    }

    @Override
    public void dagFinished(DAGJob dagJob) {
        if (!dagJob.isFinished()) {
            throw new RuntimeException("DAG not finished");
        }
        dagsFinished += 1;
        actualDagFinishTime = Math.max(cloudsim.clock(), actualDagFinishTime);
    }

    @Override
    public void simulate(String logname) {
        cloudsim.init();
        // TODO(bryk): that's ugly, I know. @Mequrel - you should change this.
        new VoidStorageManager(cloudsim);

        Cloud cloud = new Cloud(cloudsim);
        WorkflowEngine engine = new WorkflowEngine(this, this, cloudsim);
        EnsembleManager manager = new EnsembleManager(engine, cloudsim);

        setCloud(cloud);
        setEnsembleManager(manager);
        setWorkflowEngine(engine);

        WorkflowLog log = null;
        if (shouldGenerateLog()) {
            log = new WorkflowLog(cloudsim);
            engine.addJobListener(log);
            cloud.addVMListener(log);
            manager.addDAGJobListener(log);
        }

        planningStartWallTime = System.nanoTime();

        plan();

        simulationStartWallTime = System.nanoTime();

        cloudsim.startSimulation();

        simulationFinishWallTime = System.nanoTime();

        // Sanity checks
        if (dagsFinished < numCompletedDAGs()) {
            throw new RuntimeException("Not all DAGs completed");
        }

        if (actualDagFinishTime > getDeadline()) {
            System.err.println("WARNING: Exceeded deadline: " + actualDagFinishTime + ">" + getDeadline());
        }

        if (getActualCost() > getBudget()) {
            System.err.println("WARNING: Cost exceeded budget: " + getActualCost() + ">" + getBudget());
        }

        if (shouldGenerateLog()) {
            log.printJobs(logname);
            log.printVmList(logname);
            log.printDAGJobs();
        }
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

    static int nextresourceid = 0;

    class Resource {
        int id = nextresourceid++;
        VMType vmtype;
        TreeMap<Double, Slot> schedule;

        public Resource(Resource other) {
            this(other.vmtype);
            for (Double s : other.schedule.navigableKeySet()) {
                schedule.put(s, other.schedule.get(s));
            }
        }

        public Resource(VMType type) {
            this.vmtype = type;
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
            return last + lastSlot.duration + getEstimatedDeprovisioningDelay(); // add estimate of deprovisioning time
        }

        public int getHours() {
            return getHoursWith(getStart(), getEnd());
        }

        public int getHoursWith(double start, double end) {
            double seconds = end - start;
            double hours = seconds / (60 * 60);
            int rounded = (int) Math.ceil(hours);
            return Math.max(1, rounded);
        }

        public double getCostWith(double start, double end) {
            return getHoursWith(start, end) * vmtype.price;
        }

        public double getCost() {
            return getHours() * vmtype.price;
        }

        public double getUtilization() {
            double runtime = 0.0;
            for (Slot sl : schedule.values()) {
                runtime += sl.duration;
            }
            return runtime / (getHours() * 60 * 60);
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
