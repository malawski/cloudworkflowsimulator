package cws.core;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;

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
    
    private HashSet<JobListener> jobListeners = new HashSet<JobListener>();
    
    /** The provisioner that allocates resources for this workflow engine */
    private Provisioner provisioner;
    
    /** The scheduler that matches jobs to resources for this workflow engine */
    private Scheduler scheduler;
    
    /** The current VMs */
    private LinkedList<VM> vms = new LinkedList<VM>();
    
    /** The list of unmatched ready jobs */
    private LinkedList<Job> queue = new LinkedList<Job>();
    
    public WorkflowEngine(Provisioner provisioner, Scheduler scheduler) {
        super("WorkflowEngine"+(next_id++));
        this.provisioner = provisioner;
        this.scheduler = scheduler;
        CloudSim.addEntity(this);
    }
    
    public List<Job> getQueuedJobs() {
        return queue;
    }
    
    public List<VM> getAvailableVMs() {
        return vms;
    }
    
    @Override
    public void startEntity() {
        // Do nothing
    }
    
    @Override
    public void processEvent(SimEvent ev) {
        switch (ev.getTag()) {
            case VM_LAUNCHED:
                vmLaunched((VM)ev.getData());
                break;
            case VM_TERMINATED:
                vmTerminated((VM)ev.getData());
                break;
            case DAG_SUBMIT:
                dagSubmit((DAGJob)ev.getData());
                break;
            case JOB_STARTED:
                jobStarted((Job)ev.getData());
                break;
            case JOB_FINISHED:
                jobFinished((Job)ev.getData());
                break;
            default:
                throw new RuntimeException("Unrecognized event: "+ev);
        }
    }
    
    @Override
    public void shutdownEntity() {
        // Do nothing
    }
    
    private void vmLaunched(VM vm) {
        vms.add(vm);
    }
    
    private void vmTerminated(VM vm) {
        vms.remove(vm);
    }
    
    private void dagSubmit(DAGJob dj) {
        
        dags.add(dj);
        
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
            Job j = new Job(dj, t, getId());
            queue.add(j);
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
        for (JobListener jl : jobListeners) {
            jl.jobFinished(j);
        }
        
        DAGJob dj = j.getDAGJob();
        Task t = j.getTask();
        
        // If the job succeeded
        if (j.getResult() == Job.Result.SUCCESS) {
            
            // Mark the task as complete in the DAG
            dj.completeTask(t);
            
            // Queue any jobs that are now ready
            queueReadyJobs(dj);
            
            // If the workflow is complete, send it back
            if (dj.isFinished()) {
                dags.remove(dj);
                sendNow(dj.getOwner(), DAG_FINISHED, dj);
            }
        }
        
        // If the job failed
        if (j.getResult() == Job.Result.FAILURE) {
            // Retry the job
            Job retry = new Job(dj, t, getId());
            queue.add(retry);
        }
    }
}
