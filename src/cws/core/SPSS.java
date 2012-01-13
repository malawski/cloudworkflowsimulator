package cws.core;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.HashMap;
import java.util.Queue;
import java.util.SortedSet;
import java.util.TreeMap;

import org.cloudbus.cloudsim.core.CloudSim;

import cws.core.Job.Result;
import cws.core.dag.DAG;
import cws.core.dag.Task;
import cws.core.dag.DAGParser;
import cws.core.dag.algorithms.CriticalPath;
import cws.core.dag.algorithms.TopologicalOrder;
import cws.core.log.WorkflowLog;

/**
 * @author Gideon Juve <juve@usc.edu>
 */
public class SPSS implements WorkflowEvent, Provisioner, Scheduler, VMListener, JobListener, DAGJobListener {
    
    private WorkflowEngine engine;
    private EnsembleManager manager;
    private Cloud cloud;
    private double budget;
    private double ensembleDeadline;
        
    private Plan plan = new Plan();
    
    /** List of all dags in ensemble */
    private List<DAG> allDAGs;
    
    /** List of DAGs that were admitted to run */
    private List<DAG> admittedDAGs = new LinkedList<DAG>();
    
    /** Set of jobs that are ready to run arranged by Task */
    private HashMap<Task, Job> readyJobs = new HashMap<Task, Job>();
    
    /** Mapping of Task to the VM that will run the task */
    private HashMap<Task, VM> taskMap = new HashMap<Task, VM>();
    
    /** Schedule of tasks for each VM */
    private HashMap<VM, Queue<Task>> vmQueues = new HashMap<VM, Queue<Task>>();
    
    /** Set of idle VMs */
    private HashSet<VM> idle = new HashSet<VM>();
    
    /** Tuning parameter for deadline distribution (low alpha = runtime, high alpha = tasks) */
    private double alpha;
    
    public SPSS(double budget, double deadline, List<DAG> dags, double alpha) {
        this.budget = budget;
        this.ensembleDeadline = deadline;
        this.allDAGs = dags;
        this.alpha = alpha;
    }
    
    public void setCloud(Cloud c) {
        if (cloud != null)
            cloud.removeVMListener(this);
        cloud = c;
        cloud.addVMListener(this);
    }
    
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
    
    /** Return the score for this (ensemble, budget, deadline) */
    public double getScore() {
        double score = 0.0;
        for (DAG dag : admittedDAGs) {
            int index = allDAGs.indexOf(dag);
            int priority = allDAGs.size() - index - 1;
            score += Math.pow(2, priority);
        }
        return score;
    }
    
    public double getDeadline() {
        return ensembleDeadline;
    }
    
    public double getActualFinish() {
        double finish = 0.0;
        for (VM vm : vmQueues.keySet()) {
            finish = Math.max(finish, vm.getTerminateTime());
        }
        return finish;
    }
    
    public double getBudget() {
        return budget;
    }
    
    public double getPlanCost() {
        return plan.getCost();
    }
    
    public double getActualCost() {
        return engine.getCost();
    }
    
    /**
     * Develop a plan for running as many DAGs as we can
     */
    public void plan() {
        // We assume the dags are in priority order
        for (DAG dag : allDAGs) {
            try {
                Plan newPlan = planDAG(dag, plan);
                // Plan was feasible
                if (newPlan.getCost() <= budget) {
                    admittedDAGs.add(dag);
                    plan = newPlan;
                    System.out.println("Admitting DAG. Cost of new plan: "+plan.getCost());
                } else {
                    System.out.println("Rejecting DAG: New plan exceeds budget: "+newPlan.getCost());
                }
            } catch (NoFeasiblePlan m) {
                System.out.println("Rejecting DAG: "+m.getMessage());
            }
        }
        
        for (Resource r : plan.resources) {
            // Create VM
            VMType type = r.vmtype;
            VM vm = new VM(type.mips, 1, 1, type.price);
            vm.setProvisioningDelay(0.0);
            vm.setDeprovisioningDelay(0.0);
            
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
        
        // Sanity check
        if (admittedDAGs.size() == 0) {
            throw new RuntimeException("No DAGs admitted");
        }
        
        // Submit admitted DAGs
        for (DAG dag : admittedDAGs) {
            submitDAG(dag);
        }
    }
    
    /**
     * Develop a plan for a single DAG
     */
    Plan planDAG(DAG dag, Plan currentPlan) throws NoFeasiblePlan {
        TopologicalOrder order = new TopologicalOrder(dag);
        
        // Initial task assignment
        HashMap<Task, VMType> vmTypes = new HashMap<Task, VMType>();
        HashMap<Task, Double> runtimes = new HashMap<Task, Double>();
        double minCost = 0.0;
        double totalRuntime = 0.0;
        for (Task t : order) {
            
            // Initially we assign each VM to the SMALL type
            VMType vm = VMType.SMALL;
            vmTypes.put(t, vm);
            
            // The runtime is just the size of the task (MI) divided by the
            // MIPS of the VM
            double runtime = t.size / vm.mips;
            runtimes.put(t, runtime);
            
            // Compute the minimum cost of running this workflow
            minCost += (runtime/(60*60)) * vm.price;
            totalRuntime += runtime;
        }
        
        System.out.println(" Min Cost: "+minCost);
        System.out.println(" Total Runtime: "+totalRuntime);
        
        // Make sure a plan is feasible given the deadline and available VMs
        // FIXME Later we will assign each task to its fastest VM type before this
        CriticalPath path = new CriticalPath(order, runtimes);
        double criticalPath = path.getCriticalPathLength();
        System.out.println(" Critical path: "+criticalPath);
        if (criticalPath > ensembleDeadline) {
            throw new NoFeasiblePlan(
                    "Best critical path ("+criticalPath+") " +
                    "> deadline ("+ensembleDeadline+")");
        }
        
        /* FIXME Later we will determine the best VM type for each task
         *  assignEachTaskToCheapestResource()
         *  criticalPath = computeCriticalPath()
         *  while (criticalPath > deadline) {
         *      upgradeTaskWithBestBangForBuck()
         *      criticalPath = computeCriticalPath()
         *  }
         */
        
        
        Plan best = null;
        
        double LST = ensembleDeadline - criticalPath;
        
        for (double EST = 0.0; EST <= LST; EST += 3600.0) {
            Plan newPlan = planDAG(dag, currentPlan, runtimes, vmTypes, EST);
            if (best == null) {
                best = newPlan;
            } else {
                if (newPlan.getCost() < best.getCost()) {
                    best = newPlan;
                }
            }
        }
        
        return best;
    }
    
    Plan planDAG(DAG dag, Plan currentPlan, HashMap<Task, Double> runtimes, HashMap<Task, VMType> vmTypes, double EST) {
        
        TopologicalOrder order = new TopologicalOrder(dag);
        
        // Get deadlines for each task (deadline distribution)
        final HashMap<Task, Double> deadlines = 
                deadlineDistributionEST(order, runtimes, this.alpha, EST);
        
        // Sort tasks by deadline
        LinkedList<Task> sortedTasks = new LinkedList<Task>();
        for (Task t : order) {
            sortedTasks.add(t);
        }
        Comparator<Task> deadlineComparator = new Comparator<Task>() {
            @Override
            public int compare(Task t1, Task t2) {
                double d1 = deadlines.get(t1);
                double d2 = deadlines.get(t2);
                if (d1 < d2) {
                    return -1;
                } else if (d1 > d2) {
                    return 1;
                } else {
                    return 0;
                }
            }
        };
        Collections.sort(sortedTasks, deadlineComparator);
        
        // Create a new plan
        Plan plan = new Plan(currentPlan);
        
        // Actual finish times of tasks
        HashMap<Task, Double> finishTimes = new HashMap<Task, Double>();
        
        // Assign resources to each task
        for (Task t :sortedTasks) {
            double runtime = runtimes.get(t);
            double deadline = deadlines.get(t);
            VMType vmtype = vmTypes.get(t);
            
            // Compute earliest start time of task
            double earliestStart = 0.0;
            for (Task p : t.parents) {
                earliestStart = Math.max(earliestStart, finishTimes.get(p));
            }
            
            // Best scheduling solution for task
            Solution best;
            {
                // Default is to allocate a new resource
                Resource r = new Resource(vmtype);
                double cost = r.getCostWith(earliestStart, earliestStart+runtime);
                Slot sl = new Slot(t, earliestStart, runtime);
                best = new Solution(r, sl, cost);
            }
            
            // Check each resource for a better (cheaper, earlier) solution
            for (Resource r : plan.resources) {
                
                // The resource must match the vm type of the task
                if (vmtype != r.vmtype) {
                    continue;
                }
                
                // Try placing task at the beginning of resource schedule
                if (earliestStart + runtime < r.getStart()) {
                    
                    // Option 1: Leave no gap
                    nogap: {                        
                        double ast = r.getStart() - runtime;
                        if (ast < earliestStart) {
                            break nogap;
                        }
                        
                        double aft = ast + runtime;
                        if (aft > deadline || aft > r.getStart()) {
                            break nogap;
                        }
                        
                        double cost = r.getCostWith(ast, r.getEnd()) - r.getCost();
                        Slot sl = new Slot(t, ast, runtime);
                        Solution soln = new Solution(r, sl, cost);
                        if (soln.betterThan(best)) {
                            best = soln;
                        }
                    }
                    
                    // Option 2: Leave a big gap
                    biggap: {
                        int runtimeHours = (int)Math.ceil(runtime / (60*60));
                        
                        double ast = r.getStart() - (runtimeHours*60*60);
                        if (ast < earliestStart) {
                            ast = earliestStart;
                        }
                        
                        double aft = ast + runtime;
                        if (aft > deadline || aft > r.getStart()) {
                            break biggap;
                        }
                        
                        double cost = r.getCostWith(ast, r.getEnd()) - r.getCost();
                        Slot sl = new Slot(t, ast, runtime);
                        Solution soln = new Solution(r, sl, cost);
                        if (soln.betterThan(best)) {
                            best = soln;
                        }
                    }
                    
                    // Option 3: Use some slack time (medium gap)
                    slack: {
                        double slack = (r.getHours()*60*60) - (r.getEnd()-r.getStart());
                        
                        double ast = r.getStart() - slack;
                        if (ast < earliestStart) {
                            ast = earliestStart;
                        }
                        
                        double aft = ast + runtime;
                        if (aft > deadline || aft > r.getStart()) {
                            break slack;
                        }
                        
                        // This solution should be free because we add no hours
                        double cost = r.getCostWith(ast, r.getEnd()) - r.getCost();
                        if (cost > 1e-6) {
                            throw new RuntimeException("Solution should be free");
                        }
                        Slot sl = new Slot(t, ast, runtime);
                        Solution soln = new Solution(r, sl, cost);
                        if (soln.betterThan(best)) {
                            best = soln;
                        }
                    }
                }
                
                // Try placing it in a gap in the schedule
                double lastEnd = -1;
                gap: for (double start : r.getStartTimes()) {
                    double begin = lastEnd;
                    double end = start;
                    
                    lastEnd = start+r.schedule.get(start).duration;
                    
                    // This just skips the first occupied slot
                    if (begin < 0) {
                        continue gap;
                    }
                    
                    if (begin == end) {
                        continue gap;
                    }
                    
                    // Sanity check
                    if (begin > end && begin-end > 1e-9) {
                        throw new RuntimeException("Negative sized empty slot");
                    }
                    
                    double ast;
                    if (begin >= earliestStart) {
                        ast = begin;
                    } else {
                        ast = earliestStart;
                    }
                    
                    double aft = ast+runtime;
                    if (aft <= end && aft <= deadline) {
                        double cost = 0.0; // free as in beer
                        Slot sl = new Slot(t, ast, runtime);
                        Solution soln = new Solution(r, sl, cost);
                        
                        if (soln.betterThan(best))
                            best = soln;
                        
                        // We won't find a better solution by looking at later
                        // gaps, so we can stop looking here
                        break gap;
                    }
                }
                
                // Try to placing it at the end of the schedule
                atend: if (r.getEnd() + runtime < deadline) {
                    
                    // Actual start time
                    double ast;
                    if (earliestStart < r.getEnd()) {
                        ast = r.getEnd();
                    } else {
                        ast = earliestStart;
                    }
                    
                    // Actual finish time
                    double aft = ast+runtime;
                    if (aft > deadline) {
                        break atend;
                    }
                    
                    double cost = r.getCostWith(r.getStart(), aft) - r.getCost();
                    Slot sl = new Slot(t, ast, runtime);
                    
                    Solution soln = new Solution(r, sl, cost);
                    if (soln.betterThan(best))
                        best = soln;
                }
            }
            
            // Schedule task on resource of best solution
            best.addToPlan(plan);
            
            // Save actual finish time of task
            finishTimes.put(t, best.slot.start + runtime);
        }
        
        return plan;
    }
    
    /**
     * Assign deadlines to each task in the DAG
     */
    HashMap<Task, Double> deadlineDistributionEST(TopologicalOrder order, HashMap<Task, Double> runtimes, double alpha, double EST) {
            // Sanity check
            if (alpha < 0 || alpha > 1) {
                throw new RuntimeException(
                        "Invalid alpha: "+alpha+". Valid range is [0,1].");
            }
            
            // The level of each task is max[p in parents](p.level) + 1
            HashMap<Task, Integer> levels = new HashMap<Task, Integer>();
            int numlevels = 0;
            for (Task t : order) {
                int level = 0;
                for (Task p : t.parents) {
                    int plevel = levels.get(p);
                    level = Math.max(level, plevel+1);
                }
                levels.put(t, level);
                numlevels = Math.max(numlevels, level+1);
            }
            
            /* Compute:
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
            
            /* The excess time share for each level is:
             * 
             *          //         tasksInLevel \   /             runtimeInLevel \\
             *  frac =  || alpha * ------------ | + | (1-alpha) * -------------- ||
             *          \\          totalTasks  /   \              totalRuntime  //
             * 
             *  share = frac * (deadline - critical_path)
             * 
             * In other words, each level gets a fraction of the spare time that is 
             * proportional to the combination of the number of tasks it has as well
             * as the total runtime of those tasks.
             */
            double[] shares = new double[numlevels];
            CriticalPath path = new CriticalPath(order, runtimes);
            double criticalPathLength = path.getCriticalPathLength();
            double spare = this.ensembleDeadline - (criticalPathLength + EST);
            for (int i=0; i<numlevels; i++) {
                
                double taskPart = alpha * (totalTasksByLevel[i]/totalTasks);
                double runtimePart = (1-alpha) * (totalRuntimesByLevel[i]/totalRuntime);
                
                shares[i] = (taskPart + runtimePart) * spare;
            }
            
            /* The deadline of a task t is:
             * 
             * t.deadline = max[p in t.parents](p.deadline) + t.runtime + shares[t.level]
             */
            HashMap<Task, Double> deadlines = new HashMap<Task, Double>();
            for (Task t : order) {
                int level = levels.get(t);
                double latestDeadline = EST;
                for (Task p : t.parents) {
                    double pdeadline = deadlines.get(p);
                    latestDeadline = Math.max(latestDeadline, pdeadline);
                }
                double runtime = runtimes.get(t);
                double deadline = latestDeadline + runtime + shares[level];
                deadlines.put(t, deadline);
            }
            
            return deadlines;
        }
    
    HashMap<Task, Double> deadlineDistributionLST(TopologicalOrder order, HashMap<Task, Double> runtimes, double alpha) {
        HashMap<Task,Double> deadlines = new HashMap<Task,Double>();
        for (Task t : order.reverse()) {
            double deadline = ensembleDeadline;
            for (Task c : t.children) {
                deadline = Math.min(deadline, deadlines.get(c)-runtimes.get(c));
            }
            deadlines.put(t, deadline);
        }
        
        for (Task t : order) {
            System.out.println(t.id+": "+deadlines.get(t)); 
        }
        
        return deadlines;
    }
    
    HashMap<Task, Double> deadlineDistribution(TopologicalOrder order, HashMap<Task, Double> runtimes, double alpha) {
        
        // Sanity check
        if (alpha < 0 || alpha > 1) {
            throw new RuntimeException(
                    "Invalid alpha: "+alpha+". Valid range is [0,1].");
        }
        
        // The level of each task is max[p in parents](p.level) + 1
        HashMap<Task, Integer> levels = new HashMap<Task, Integer>();
        int numlevels = 0;
        for (Task t : order) {
            int level = 0;
            for (Task p : t.parents) {
                int plevel = levels.get(p);
                level = Math.max(level, plevel+1);
            }
            levels.put(t, level);
            numlevels = Math.max(numlevels, level+1);
        }
        
        /* Compute:
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
        
        /* The excess time share for each level is:
         * 
         *          //         tasksInLevel \   /             runtimeInLevel \\
         *  frac =  || alpha * ------------ | + | (1-alpha) * -------------- ||
         *          \\          totalTasks  /   \              totalRuntime  //
         * 
         *  share = frac * (deadline - critical_path)
         * 
         * In other words, each level gets a fraction of the spare time that is 
         * proportional to the combination of the number of tasks it has as well
         * as the total runtime of those tasks.
         */
        double[] shares = new double[numlevels];
        CriticalPath path = new CriticalPath(order, runtimes);
        double criticalPathLength = path.getCriticalPathLength();
        double spare = this.ensembleDeadline - criticalPathLength;
        for (int i=0; i<numlevels; i++) {
            
            double taskPart = alpha * (totalTasksByLevel[i]/totalTasks);
            double runtimePart = (1-alpha) * (totalRuntimesByLevel[i]/totalRuntime);
            
            shares[i] = (taskPart + runtimePart) * spare;
        }
        
        /* The deadline of a task t is:
         * 
         * t.deadline = max[p in t.parents](p.deadline) + t.runtime + shares[t.level]
         */
        HashMap<Task, Double> deadlines = new HashMap<Task, Double>();
        for (Task t : order) {
            int level = levels.get(t);
            double latestDeadline = 0.0;
            for (Task p : t.parents) {
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
        DAGJob dagJob = new DAGJob(dag, manager.getId());
        int priority = allDAGs.indexOf(dag);
        dagJob.setPriority(priority);
        CloudSim.send(manager.getId(), engine.getId(), 0.0, DAG_SUBMIT, dagJob);
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
    public void jobSubmitted(Job job) { }

    @Override
    public void jobStarted(Job job) { }

    @Override
    public void jobFinished(Job job) {
        if (job.getResult() != Result.SUCCESS) {
            // FIXME What if the job failed?
            // We need to re-queue the task
            throw new RuntimeException("Job failed!");
        }
        
        // Sanity check
        DAG dag = job.getDAGJob().getDAG();
        if (!admittedDAGs.contains(dag)) {
            throw new RuntimeException("Running DAG that wasn't accepted");
        }
        
        VM vm = job.getVM();
        
        idle.add(vm);
        submitNextTaskFor(vm);
    }
    
    private void submitNextTaskFor(VM vm) {
        // If the VM is busy, do nothing
        if (!idle.contains(vm))
            return;
        
        Queue<Task> vmqueue = vmQueues.get(vm);
        
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
        double now = CloudSim.clock();
        double delay = start - now;
        CloudSim.send(engine.getId(), cloud.getId(), delay, VM_LAUNCH, vm);
    }
    
    private void terminateVM(VM vm) {
        CloudSim.send(engine.getId(), cloud.getId(), 0.0, VM_TERMINATE, vm);
    }
    
    private void submitJob(VM vm, Job job) {
        Task task = job.getTask();
        
        // Advance queue
        Queue<Task> vmqueue = vmQueues.get(vm);
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
        CloudSim.send(engine.getId(), vm.getId(), 0.0, JOB_SUBMIT, job);
    }
    
    @Override
    public void dagStarted(DAGJob dagJob) {
        /* Do nothing */
    }
    
    @Override
    public void dagFinished(DAGJob dagJob) {
        /* Do nothing */
    }
    
    enum VMType {
        SMALL(1, 1.0),
        MEDIUM(5, 0.40),
        LARGE(10, 0.80);
        
        int mips;
        double price;
        
        VMType(int mips, double price) {
            this.mips = mips;
            this.price = price;
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
    
    class Resource {
        VMType vmtype;
        TreeMap<Double,Slot> schedule;
        
        public Resource(Resource other) {
            this(other.vmtype);
            for (Double s : other.schedule.navigableKeySet()) {
                schedule.put(s, other.schedule.get(s));
            }
        }
        
        public Resource(VMType type) {
            this.vmtype = type;
            this.schedule = new TreeMap<Double,Slot>();
        }
        
        public SortedSet<Double> getStartTimes() {
            return schedule.navigableKeySet();
        }
        
        public double getStart() {
            if (schedule.size() == 0) {
                throw new RuntimeException("No scheduled tasks");
            }
            return schedule.firstKey();
        }
        
        public double getEnd() {
            if (schedule.size() == 0) {
                throw new RuntimeException("No scheduled tasks");
            }
            double last = schedule.lastKey();
            Slot lastSlot = schedule.get(last);
            return last + lastSlot.duration;
        }
        
        public int getHours() {
            return getHoursWith(getStart(), getEnd());
        }
        
        public int getHoursWith(double start, double end) {
            double seconds = end - start;
            double hours = seconds / (60*60);
            int rounded = (int)Math.ceil(hours);
            return rounded;
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
            return runtime / (getHours()*60*60);
        }
    }
    
    class Solution {
        double cost;
        Resource resource;
        Slot slot;
        
        public Solution(Resource resource, Slot slot, double cost) {
            this.resource = resource;
            this.slot = slot;
            this.cost = cost;
        }
        
        public boolean betterThan(Solution other) {
            // A solution is better than no solution
            if (other == null)
                return true;
            
            // Cheaper solutions are better
            if (this.cost < other.cost)
                return true;
            
            // Existing resources are better
            if (this.resource.schedule.size() == 0 && other.resource.schedule.size() > 0)
                return false;
            if (this.resource.schedule.size() > 0 && other.resource.schedule.size() == 0)
                return true;
            
            // Earlier starts are better
            if (this.slot.start < other.slot.start)
                return true;
            
            return false;
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
    
    public static void main(String[] args) throws Exception {
        CloudSim.init(1, null, false);
        
        List<DAG> dags = new ArrayList<DAG>();
        
        File ens = new File("dags/ensemble2.txt");
        BufferedReader reader = new BufferedReader(new FileReader(ens));
        for (String line = reader.readLine(); line!= null; line = reader.readLine()) {
            DAG dag = DAGParser.parseDAG(new File(line));
            dags.add(dag);
        }
        
        /*
        
        for (int i = 0; i < 50; i++) {
            DAG dag = DAGParser.parseDAG(new File("../projects/pegasus/CyberShake/CYBERSHAKE.n.1000.8.dag"));
            dags.add(dag);
        }
        */
        
        double deadline = 10*3600;
        double budget = 120;
        double alpha = 0.7;
        
        SPSS spss = new SPSS(budget, deadline, dags, alpha);
        
        Cloud cloud = new Cloud();
        WorkflowEngine engine = new WorkflowEngine(spss, spss);
        EnsembleManager manager = new EnsembleManager(engine);
        
        spss.setCloud(cloud);
        spss.setEnsembleManager(manager);
        spss.setWorkflowEngine(engine);
        
        WorkflowLog log = new WorkflowLog();
        engine.addJobListener(log);
        cloud.addVMListener(log);
        manager.addDAGJobListener(log);
        
        spss.plan();
        
        try {
            //Thread.sleep(10000);
        } catch (Exception e) {
            
        }
        
        CloudSim.startSimulation();
        
        String fName = "testSPSSSPSSMontage_25.dag"+"x"+dags.size()+"d"+deadline+"b"+budget+"m0";
        log.printJobs(fName);
        log.printVmList(fName);
        log.printDAGJobs();
        
        System.out.println("Workflows Completed: "+spss.admittedDAGs.size());
        
        System.out.println("Budget: "+spss.getBudget());
        System.out.println("Plan Cost: "+spss.getPlanCost());
        System.out.println("Actual Cost: "+spss.getActualCost());
        
        System.out.println("Deadline: "+spss.getDeadline());
        System.out.println("Finish time: "+spss.getActualFinish());
        
        if (spss.readyJobs.size() > 0) {
            throw new RuntimeException("Ready tasks remain");
        }
    }
}
