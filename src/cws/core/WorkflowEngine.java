package cws.core;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;

import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.SimEntity;
import org.cloudbus.cloudsim.core.SimEvent;

import cws.core.dag.Task;

/**
 * The workflow engine is an entity that executes workflows by scheduling their
 * tasks on VMs.
 * 
 * @author Gideon Juve <juve@usc.edu>
 */
public class WorkflowEngine extends SimEntity implements WorkflowEvent {
    public static int next_id = 0;

    private LinkedList<DAGJob> dags = new LinkedList<DAGJob>();

    /** XXX How is this different than the one above? */
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
    protected Set<VM> busyVMs = new HashSet<VM>();

    /** The list of unmatched ready jobs */
    private LinkedList<Job> queue = new LinkedList<Job>();

    /**
     * A factory for creating Job objects from Task objects
     */
    private JobFactory jobFactory = null;

    /**
     * The value that is used by provisioner to estimate system load
     * FIXME This seems unnecessary
     */
    private int queueLength = 0;

    /**
     * Deadline
     * XXX Why should the workflow engine know about the deadline?
     */
    private double deadline = Double.MAX_VALUE;

    /**
     * Budget
     * XXX Why should the workflow engine know about the budget?
     */
    double budget = Double.MAX_VALUE;

    public WorkflowEngine(JobFactory jobFactory, Provisioner provisioner, Scheduler scheduler) {
        super("WorkflowEngine" + (next_id++));
        this.jobFactory = jobFactory;
        this.provisioner = provisioner;
        this.scheduler = scheduler;
        CloudSim.addEntity(this);
    }

    public WorkflowEngine(Provisioner provisioner, Scheduler scheduler) {
        this(new SimpleJobFactory(), provisioner, scheduler);
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

    @Override
    public void startEntity() {
        // send the first provisioning request
        // it should be sent after dag submissions events
        send(this.getId(), 0.0001, PROVISIONING_REQUEST);
    }

    @Override
    public void processEvent(SimEvent ev) {
        switch (ev.getTag()) {
        case VM_LAUNCHED:
            vmLaunched((VM) ev.getData());
            break;
        case VM_TERMINATED:
            vmTerminated((VM) ev.getData());
            break;
        case DAG_SUBMIT:
            dagSubmit((DAGJob) ev.getData());
            break;
        case JOB_STARTED:
            jobStarted((Job) ev.getData());
            break;
        case JOB_FINISHED:
            jobFinished((Job) ev.getData());
            break;
        case PROVISIONING_REQUEST:
            if (provisioner != null)
                if (vms.size() > 0 || dags.size() > 0)
                    provisioner.provisionResources(this);
            break;
        default:
            throw new RuntimeException("Unrecognized event: " + ev);
        }
    }

    @Override
    public void shutdownEntity() {
        // Do nothing
    }

    /**
     * XXX Why does the workflow engine do this?
     * @return total cost consumed by all VMs.
     */
    public double getCost() {

        double cost = 0;

        for (VM vm : vms) {
            cost += vm.getCost();
        }
        return cost;
    }

    private void vmLaunched(VM vm) {
        vms.add(vm);
        freeVMs.add(vm);
        scheduler.scheduleJobs(this);
    }

    private void vmTerminated(VM vm) {
        /* Do nothing */
    }

    private void dagSubmit(DAGJob dj) {

        dags.add(dj);
        allDAGJobs.add(dj);

        // The DAG starts immediately
        sendNow(dj.getOwner(), DAG_STARTED, dj);

        // Queue any ready jobs for this DAG
        queueReadyJobs(dj);
    }

    private void queueReadyJobs(DAGJob dj) {
        // Get the ready tasks and convert them into jobs
        while (true) {
            Task t = dj.nextReadyTask();
            if (t == null)
                break;
            Job j = jobFactory.createJob(dj, t, getId());
            j.setDAGJob(dj);
            j.setTask(t);
            j.setOwner(getId());
            jobReleased(j);
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
    }

    private void jobFinished(Job j) {
        // Notify the listeners
        // IT IS IMPORTANT THAT THIS HAPPENS FIRST
        for (JobListener jl : jobListeners) {
            jl.jobFinished(j);
        }

        DAGJob dj = j.getDAGJob();
        Task t = j.getTask();

        // If the job succeeded
        if (j.getResult() == Job.Result.SUCCESS && CloudSim.clock() <= deadline) {

            // Mark the task as complete in the DAG
            dj.completeTask(t);

            // Queue any jobs that are now ready
            queueReadyJobs(dj);

            // If the workflow is complete, send it back
            if (dj.isFinished()) {
                dags.remove(dj);
                sendNow(dj.getOwner(), DAG_FINISHED, dj);
            }

            Log.printLine(CloudSim.clock() + " Job " + j.getID() + " finished on VM " + j.getVM().getId());
            VM vm = j.getVM();
            // add to free if contained in busy set
            if (busyVMs.remove(vm))
                freeVMs.add(vm);
        }

        // If the job failed
        if (j.getResult() == Job.Result.FAILURE) {
            // Retry the job
            Log.printLine(CloudSim.clock() + " Job " + j.getID() + " failed on VM " + j.getVM().getId()
                    + " resubmitting...");
            Job retry = jobFactory.createJob(dj, t, getId());
            VM vm = j.getVM();
            // add to free if contained in busy set
            if (busyVMs.remove(vm))
                freeVMs.add(vm);
            jobReleased(retry);
        }

        scheduler.scheduleJobs(this);
    }
}
