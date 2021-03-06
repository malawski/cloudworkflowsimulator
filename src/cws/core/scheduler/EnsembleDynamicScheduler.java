package cws.core.scheduler;

import java.util.*;

import cws.core.VM;
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

        List<Job> jobs = engine.getAndClearReleasedJobs();
        prioritizedJobs.addAll(jobs);

        // use prioritized list for scheduling
        scheduleQueue(prioritizedJobs, engine);
    }

    protected void scheduleQueue(Queue<Job> jobs, WorkflowEngine engine) {
        List<VM> freeVMs = new ArrayList<VM>(engine.getFreeVMs());
        while (!jobs.isEmpty() && !freeVMs.isEmpty()) {
            Job job = jobs.poll();
            VM vm = freeVMs.remove(freeVMs.size() - 1);
            vm.jobSubmit(job);
        }
    }

    /**
     * Compares jobs based on their priority.
     */
    private static final class JobPriorityComparator implements Comparator<Job> {
        @Override
        public int compare(Job job1, Job job2) {
            final int p1 = job1.getDAGJob().getPriority();
            final int p2 = job2.getDAGJob().getPriority();
            return (p1 < p2) ? -1 : ((p1 == p2) ? 0 : 1);
        }
    }
}
