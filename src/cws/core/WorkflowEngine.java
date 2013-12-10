package cws.core;

import java.util.*;

import cws.core.cloudsim.CWSSimEntity;
import cws.core.cloudsim.CWSSimEvent;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.dag.DAGJob;
import cws.core.dag.Task;
import cws.core.jobs.Job;
import cws.core.jobs.JobFactory;
import cws.core.jobs.JobListener;
import cws.core.jobs.SimpleJobFactory;

/**
 * The workflow engine is an entity that executes workflows by scheduling their
 * tasks on VMs.
 * 
 * @author Gideon Juve <juve@usc.edu>
 */
public class WorkflowEngine extends CWSSimEntity {
    public static int next_id = 0;

    private LinkedList<DAGJob> dags = new LinkedList<DAGJob>();

    /**
     * XXX How is this different than the one above?
     * TODO(bryk): anyone can answer this question?
     */
    private LinkedList<DAGJob> allDAGJobs = new LinkedList<DAGJob>();

    private HashSet<JobListener> jobListeners = new HashSet<JobListener>();

    /** The provisioner that allocates resources for this workflow engine */
    private Provisioner provisioner;

    /** The scheduler that matches jobs to resources for this workflow engine */
    private Scheduler scheduler;

    /** The current VMs */
    private LinkedList<VM> vms = new LinkedList<VM>();

    /** The set of free VMs, i.e. the ones which are not executing any jobs (idle) */
    protected Set<VM> freeVMs = new HashSet<VM>();

    /** The set of busy VMs, i.e. the ones which execute jobs */
    private Set<VM> busyVMs = new HashSet<VM>();

    /** The list of unmatched ready jobs */
    private LinkedList<Job> queue = new LinkedList<Job>();

    /**
     * A factory for creating Job objects from Task objects
     */
    private JobFactory jobFactory = null;

    /**
     * The value that is used by provisioner to estimate system load
     * FIXME This seems unnecessary
     * TODO(bryk): As far as I can see this is used in the code. So it IS necessary. I'll remove this comment if no one
     * votes against.
     */
    private int queueLength = 0;

    /**
     * Deadline
     * XXX Why should the workflow engine know about the deadline?
     * TODO(bryk): As far as I can see this is used in the code. So it IS necessary. I'll remove this comment if no one
     * votes against.
     */
    private double deadline = Double.MAX_VALUE;

    /**
     * Budget
     * XXX Why should the workflow engine know about the budget?
     * TODO(bryk): As far as I can see this is used in the code. So it IS necessary. I'll remove this comment if no one
     * votes against.
     */
    double budget = Double.MAX_VALUE;

    private boolean provisioningRequestSend = false;

    public WorkflowEngine(JobFactory jobFactory, Provisioner provisioner, Scheduler scheduler, CloudSimWrapper cloudsim) {
        super("WorkflowEngine" + (next_id++), cloudsim);
        this.jobFactory = jobFactory;
        this.provisioner = provisioner;
        this.scheduler = scheduler;
    }

    public WorkflowEngine(Provisioner provisioner, Scheduler scheduler, CloudSimWrapper cloudsim) {
        this(new SimpleJobFactory(), provisioner, scheduler, cloudsim);
    }

    @Override
    public void processEvent(CWSSimEvent ev) {
        switch (ev.getTag()) {
        case WorkflowEvent.VM_LAUNCHED:
            vmLaunched((VM) ev.getData());
            break;
        case WorkflowEvent.VM_TERMINATED:
            vmTerminated((VM) ev.getData());
            break;
        case WorkflowEvent.DAG_SUBMIT:
            dagSubmit((DAGJob) ev.getData());
            if (!this.provisioningRequestSend) {
                this.provisioningRequestSend = true;
                this.sendNow(this.getId(), WorkflowEvent.PROVISIONING_REQUEST);
            }
            break;
        case WorkflowEvent.JOB_STARTED:
            jobStarted((Job) ev.getData());
            break;
        case WorkflowEvent.JOB_FINISHED:
            jobFinished((Job) ev.getData());
            break;
        case WorkflowEvent.PROVISIONING_REQUEST:
            if (provisioner != null)
                if (vms.size() > 0 || dags.size() > 0)
                    provisioner.provisionResources(this);
            break;
        default:
            throw new RuntimeException("Unrecognized event: " + ev);
        }
    }

    public double getCost() {
        double ret = cost;
        for (VM vm : vms) {
            ret += vm.getCost();
        }
        return ret;
    }

    private double cost = 0;

    @Override
    public void shutdownEntity() {
        getCloudsim().log("Total cost: " + getCost() + ", time: " + getCloudsim().clock());
    }

    private void vmLaunched(VM vm) {
        vms.add(vm);
        freeVMs.add(vm);
        scheduler.scheduleJobs(this);
    }

    private void vmTerminated(VM vm) {
        cost += vm.getCost();
        vms.remove(vm);
        freeVMs.remove(vm);
        busyVMs.remove(vm);
    }

    private void dagSubmit(DAGJob dj) {
        dags.add(dj);
        allDAGJobs.add(dj);

        // The DAG starts immediately
        sendNow(dj.getOwner(), WorkflowEvent.DAG_STARTED, dj);

        // Queue any ready jobs for this DAG
        queueReadyJobs(dj);
    }

    private void queueReadyJobs(DAGJob dagJob) {
        // Get the ready tasks and convert them into jobs
        while (true) {
            Task task = dagJob.nextReadyTask();
            if (task == null)
                break;
            Job job = jobFactory.createJob(dagJob, task, getId(), getCloudsim());
            job.setDAGJob(dagJob);
            job.setTask(task);
            job.setOwner(getId());
            jobReleased(job);
        }
    }

    private void jobReleased(Job j) {
        queue.add(j);

        // Notify listeners that job was released
        for (JobListener jl : jobListeners) {
            jl.jobReleased(j);
        }
    }

    private void jobStarted(Job j) {
        // Notify the listeners
        for (JobListener jl : jobListeners) {
            jl.jobStarted(j);
        }
        VM vm = j.getVM();
        if (freeVMs.remove(vm))
            busyVMs.add(vm);
    }

    private void jobFinished(Job job) {
        // Notify the listeners
        // IT IS IMPORTANT THAT THIS HAPPENS FIRST
        for (JobListener jl : jobListeners) {
            jl.jobFinished(job);
        }

        DAGJob dagJob = job.getDAGJob();
        Task t = job.getTask();

        // If the job succeeded
        if (job.getResult() == Job.Result.SUCCESS && getCloudsim().clock() <= deadline) {

            // FIXME: temporary hack - when data transfer job
            if (dagJob != null) {
                // Mark the task as complete in the DAG
                dagJob.completeTask(t);

                // Queue any jobs that are now ready
                queueReadyJobs(dagJob);

                // If the workflow is complete, send it back
                if (dagJob.isFinished()) {
                    dags.remove(dagJob);
                    sendNow(dagJob.getOwner(), WorkflowEvent.DAG_FINISHED, dagJob);
                }
            }

            getCloudsim().log(job.toString() + " finished on VM " + job.getVM().getId());
            VM vm = job.getVM();
            // add to free if contained in busy set
            if (busyVMs.remove(vm))
                freeVMs.add(vm);
        } else if (job.getResult() == Job.Result.FAILURE) { // If the job failed
            // Retry the job
            getCloudsim().log(
                    String.format(
                            "Job %d (task_id = %s, workflow_id = %s, retry = %s) failed on VM %s. Resubmitting...", job
                                    .getID(), job.getTask().getId(), job.getDAGJob().getDAG().getId(), job.isRetry(),
                            job.getVM().getId()));
            Job retry = jobFactory.createJob(dagJob, t, getId(), getCloudsim());
            retry.setRetry(true);
            VM vm = job.getVM();
            // add to free if contained in busy set
            if (busyVMs.remove(vm))
                freeVMs.add(vm);
            jobReleased(retry);
        } else {
            getCloudsim().log(
                    String.format("Job %d (task_id = %s, workflow_id = %s, retry = %s) exceeded deadline.",
                            job.getID(), job.getTask().getId(), job.getDAGJob().getDAG().getId(), job.isRetry()));
            VM vm = job.getVM();
            if (busyVMs.remove(vm))
                freeVMs.add(vm);
        }

        scheduler.scheduleJobs(this);
    }

    public int getQueueLength() {
        return queueLength;
    }

    public void setQueueLength(int queueLength) {
        this.queueLength = queueLength;
    }

    public double getDeadline() {
        return deadline;
    }

    public void setDeadline(double deadline) {
        this.deadline = deadline;
    }

    public double getBudget() {
        return budget;
    }

    public void setBudget(double budget) {
        this.budget = budget;
    }

    public Queue<Job> getQueuedJobs() {
        return queue;
    }

    public List<VM> getAvailableVMs() {
        return vms;
    }

    public Set<VM> getFreeVMs() {
        return freeVMs;
    }

    public Set<VM> getBusyVMs() {
        return busyVMs;
    }

    public LinkedList<DAGJob> getAllDags() {
        return allDAGJobs;
    }

    public void addJobListener(JobListener l) {
        jobListeners.add(l);
    }

    public void removeJobListener(JobListener l) {
        jobListeners.remove(l);
    }
}
