package cws.core.scheduler;

import java.util.HashSet;
import java.util.Queue;
import java.util.Set;

import cws.core.VM;
import cws.core.WorkflowEngine;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.dag.DAG;
import cws.core.dag.DAGJob;
import cws.core.dag.Task;
import cws.core.jobs.Job;

/**
 * This scheduler submits workflow ensemble to VMs on FCFS basis.
 * Job is submitted to VM only if VM is idle (no queueing in VMs)
 * and if there are no higher priority jobs in the queue.
 * 
 * @author malawski
 */
public class WorkflowAwareEnsembleScheduler extends EnsembleDynamicScheduler {

    public WorkflowAwareEnsembleScheduler(CloudSimWrapper cloudsim) {
        super(cloudsim);
    }

    private Set<DAGJob> admittedDAGs = new HashSet<DAGJob>();
    private Set<DAGJob> rejectedDAGs = new HashSet<DAGJob>();

    @Override
    public void scheduleJobs(WorkflowEngine engine) {

        // check the deadline constraints (provisioner takes care about budget)

        double deadline = engine.getDeadline();
        double time = getCloudSim().clock();

        // stop scheduling any new jobs if we are over deadline
        if (isDeadlineExceeded(deadline, time)) {
            return;
        }

        Queue<Job> jobs = engine.getQueuedJobs();

        // move all jobs to priority queue
        prioritizedJobs.addAll(jobs);
        jobs.clear();

        // use prioritized list for scheduling
        scheduleQueue(prioritizedJobs, engine);

        // update queue length for the provisioner
        engine.setQueueLength(prioritizedJobs.size());

    }

    protected boolean isDeadlineExceeded(double deadline, double time) {
        return time >= deadline;
    }

    /**
     * Schedule all jobs from the queue to available free VMs.
     * Successfully scheduled jobs are removed from the queue.
     * @param jobs
     * @param engine
     */
    @Override
    protected void scheduleQueue(Queue<Job> jobs, WorkflowEngine engine) {
        // FIXME(_mequrel_): copying references because when we remove it from list, garbage collector removes VM...
        // imho it shouldnt working like that
        Set<VM> freeVMs = new HashSet<VM>(engine.getFreeVMs());

        while (canBeScheduled(jobs, freeVMs)) {
            Job job = jobs.poll();

            if (isJobDagAdmitted(job, engine)) {
                scheduleJob(job, freeVMs, engine);
            }
        }
    }

    private boolean isJobDagAdmitted(Job job, WorkflowEngine engine) {
        DAGJob dj = job.getDAGJob();
        
        if (jobHasBeenAlreadyAdmitted(dj)) {
            return true;
        }
        else if(jobHasBeenAlreadyRejected(dj)) {
            return false;
        }
        else {
            boolean isAdmittable = isJobAdmittable(dj, engine);
            rememberAdmitionOrRejection(dj, isAdmittable);
            return isAdmittable;
        }
        
    }

    private void rememberAdmitionOrRejection(DAGJob dj, boolean isAdmittable) {
        if(isAdmittable) {
            admittedDAGs.add(dj);    
        }
        else {
            rejectedDAGs.add(dj);
        }
    }

    protected boolean jobHasBeenAlreadyRejected(DAGJob dj) {
        return rejectedDAGs.contains(dj);
    }

    protected boolean jobHasBeenAlreadyAdmitted(DAGJob dj) {
        return admittedDAGs.contains(dj);
    }

    protected boolean canBeScheduled(Queue<Job> jobs, Set<VM> freeVMs) {
        return !freeVMs.isEmpty() && !jobs.isEmpty();
    }

    // decide what to do with the job from a new dag
    private boolean isJobAdmittable(DAGJob dj, WorkflowEngine engine) {

        double costEstimate = estimateCost(dj, engine);
        double budgetRemaining = estimateBudgetRemaining(engine);
        getCloudSim().log(" Cost estimate: " + costEstimate + " Budget remaining: "
                + budgetRemaining);
        return costEstimate < budgetRemaining;
    }

    /**
     * Estimate cost of this workflow
     * @param dj
     * @param engine
     * @return
     */
    private double estimateCost(DAGJob dj, WorkflowEngine engine) {
        double sumRuntime = sumRuntime(dj.getDAG());
        double vmPrice = vmPrice(engine);
        return vmPrice * sumRuntime / 3600.0;
    }

    /**
     * Estimate budget remaining, including unused $ and running VMs
     * TODO: compute budget consumed/remaining by already admitted workflows
     * @param dj
     * @param engine
     * @return
     */
    private double estimateBudgetRemaining(WorkflowEngine engine) {

        // remaining budget for starting new vms
        double rn = engine.getBudget() - engine.getCost();
        if (rn < 0)
            rn = 0;

        // compute remaining (not consumed) budget of currently running VMs
        double rc = 0.0;

        Set<VM> vms = new HashSet<VM>();
        vms.addAll(engine.getFreeVMs());
        vms.addAll(engine.getBusyVMs());

        for (VM vm : vms) {
            rc += vm.getCost() - vm.getRuntime() * vm.getPrice() / 3600.0;
        }

        // compute remaining runtime of admitted workflows
        double ra = 0.0;

        for (DAGJob admittedDJ : admittedDAGs) {
            if (!admittedDJ.isFinished()) {
                ra += computeRemainingCost(admittedDJ, engine);
            }
        }

        // we add this for safety in order not to underestimate our budget
        double safetyMargin = 0.1;

        getCloudSim().log(
                " Budget for new VMs: " + rn + " Budget on running VMs: " + rc
                + " Remaining budget of admitted workflows: " + ra);

        return rn + rc - ra - safetyMargin;
    }

    /**
     * Estimate remaining cost = total remaining time of incomplete tasks * price
     * 
     * @param admittedDJ
     * @param engine
     * @return
     */
    private double computeRemainingCost(DAGJob admittedDJ, WorkflowEngine engine) {
        double cost = 0.0;
        DAG dag = admittedDJ.getDAG();
        for (String taskName : dag.getTasks()) {
            Task task = dag.getTaskById(taskName);
            if (!admittedDJ.isComplete(task))
                cost += task.getSize() * vmPrice(engine);
        }
        return cost / 3600.0;
    }

    /**
     * @return the price of VM hour, assuming that all the vms are homogeneous
     */
    private double vmPrice(WorkflowEngine engine) {
        double vmPrice = 0;
        if (!engine.getAvailableVMs().isEmpty())
            vmPrice = engine.getAvailableVMs().get(0).getPrice();
        return vmPrice;
    }

    /**
     * @return The total runtime of all tasks in the workflow
     */
    public double sumRuntime(DAG dag) {
        double sum = 0.0;
        for (String taskName : dag.getTasks()) {
            sum += dag.getTaskById(taskName).getSize();
        }
        return sum;
    }
}
