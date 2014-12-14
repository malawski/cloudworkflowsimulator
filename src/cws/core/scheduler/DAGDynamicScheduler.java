package cws.core.scheduler;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

import cws.core.Scheduler;
import cws.core.VM;
import cws.core.WorkflowEngine;
import cws.core.cloudsim.CWSSimEntity;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.engine.Environment;
import cws.core.jobs.Job;

/**
 * This scheduler submits jobs to VMs on FCFS basis.
 * Job is submitted to VM only if VM is idle (no queuing in VMs).
 * @author malawski
 */
abstract class DAGDynamicScheduler extends CWSSimEntity implements Scheduler {
    protected final Environment environment;

    public DAGDynamicScheduler(CloudSimWrapper cloudsim, Environment environment) {
        super("DAGDynamicScheduler", cloudsim);
        this.environment = environment;
    }

    @Override
    public abstract void scheduleJobs(WorkflowEngine engine);

    /**
     * Schedule all jobs from the queue to available free VMs.
     * Successfully scheduled jobs are removed from the queue.
     * @param jobs
     * @param engine
     */
    protected void scheduleQueue(Queue<Job> jobs, WorkflowEngine engine) {
        List<VM> freeVMs = new ArrayList<VM>(engine.getFreeVMs());
        while (!jobs.isEmpty() && !freeVMs.isEmpty()) {
            Job job = jobs.poll();
            VM vm = freeVMs.remove(freeVMs.size() - 1);
            vm.jobSubmit(job);
        }
    }
}
