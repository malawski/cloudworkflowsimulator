package cws.core.scheduler;

import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.Queue;

import cws.core.WorkflowEngine;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.engine.Environment;
import cws.core.jobs.Job;

/**
 * This scheduler submits workflow ensemble to VMs on FCFS basis.
 * Job is submitted to VM only if VM is idle (no queueing in VMs)
 * and if there are no higher priority jobs in the queue.
 * @author malawski
 */
public class EnsembleDynamicScheduler extends DAGDynamicScheduler {
    private final PriorityQueue<Job> prioritizedJobs = new PriorityQueue<Job>(64, new JobPriorityComparator());

    public EnsembleDynamicScheduler(CloudSimWrapper cloudsim, Environment environment) {
        super(cloudsim, environment);
    }

    @Override
    public final void scheduleJobs(WorkflowEngine engine) {
        // check the deadline constraints (provisioner takes care about budget)
        double deadline = engine.getDeadline();
        double time = getCloudsim().clock();

        // stop scheduling any new jobs if we are over deadline
        if (time >= deadline) {
            return;
        }

        Queue<Job> jobs = engine.getQueuedJobs();

        moveAllJobsToPriorityQueue(jobs);

        // use prioritized list for scheduling
        scheduleQueue(prioritizedJobs, engine);
    }

    private void moveAllJobsToPriorityQueue(Queue<Job> jobs) {
        prioritizedJobs.addAll(jobs);
        jobs.clear();
    }

    /**
     * Compares jobs based on their priority
     */
    private static final class JobPriorityComparator implements Comparator<Job> {
        @Override
        public int compare(Job job1, Job job2) {
            return Integer.compare(job1.getDAGJob().getPriority(), job2.getDAGJob().getPriority());
        }
    }
}
