package cws.core.scheduler;

import java.util.Queue;

import cws.core.Scheduler;
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
    protected abstract void scheduleQueue(Queue<Job> jobs, WorkflowEngine engine);
}
