package cws.core.scheduler;

import java.util.HashSet;
import java.util.Queue;
import java.util.Set;

import cws.core.Scheduler;
import cws.core.VM;
import cws.core.WorkflowEngine;
import cws.core.WorkflowEvent;
import cws.core.cloudsim.CWSSimEntity;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.engine.Environment;
import cws.core.jobs.Job;

/**
 * This scheduler submits jobs to VMs on FCFS basis.
 * Job is submitted to VM only if VM is idle (no queuing in VMs).
 * @author malawski
 */
public class DAGDynamicScheduler extends CWSSimEntity implements Scheduler {
    protected Environment environment;

    public DAGDynamicScheduler(CloudSimWrapper cloudsim) {
        super("DAGDynamicScheduler", cloudsim);
    }

    @Override
    public void setEnvironment(Environment environment) {
        this.environment = environment;
    }

    @Override
    public void scheduleJobs(WorkflowEngine engine) {
        // use the queued (released) jobs from the workflow engine
        Queue<Job> jobs = engine.getQueuedJobs();

        scheduleQueue(jobs, engine);
    }

    /**
     * Schedule all jobs from the queue to available free VMs.
     * Successfully scheduled jobs are removed from the queue.
     * @param jobs
     * @param engine
     */
    protected void scheduleQueue(Queue<Job> jobs, WorkflowEngine engine) {
        /*
         * FIXME(_mequrel_): copying references because when we remove it from list, garbage collector removes VM...
         * imho it shouldn't working like that
         */
        Set<VM> freeVMs = new HashSet<VM>(engine.getFreeVMs());

        while (canBeScheduled(jobs, freeVMs)) {
            Job job = jobs.poll();
            scheduleJob(job, freeVMs, engine);
        }
    }

    protected void scheduleJob(Job job, Set<VM> freeVMs, WorkflowEngine engine) {
        VM vm = getFirst(freeVMs);
        markVMAsBusy(freeVMs, vm);

        job.setVM(vm);

        sendJobToVM(engine, vm, job);
    }

    private void sendJobToVM(WorkflowEngine engine, VM vm, Job job) {
        getCloudsim().send(engine.getId(), vm.getId(), 0.0, WorkflowEvent.JOB_SUBMIT, job);
        getCloudsim().log("Submitting job " + job.getTask().getId() + " to VM " + job.getVM().getId());
    }

    private boolean canBeScheduled(Queue<Job> jobs, Set<VM> freeVMs) {
        return !freeVMs.isEmpty() && !jobs.isEmpty();
    }

    private void markVMAsBusy(Set<VM> freeVMs, VM vm) {
        freeVMs.remove(vm);
    }

    private VM getFirst(Set<VM> freeVMs) {
        return freeVMs.iterator().next();
    }
}
