package cws.core.scheduler;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;

import cws.core.VM;
import cws.core.WorkflowEngine;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.engine.Environment;
import cws.core.jobs.Job;

/**
 * {@link WorkflowAwareEnsembleScheduler} implementation that is also aware of the underlying storage and schedules jobs
 * to minimize file transfers.
 */
public class WorkflowAndLocalityAwareEnsembleScheduler extends DAGDynamicScheduler {
    private final RuntimePredictioner runtimePredictioner;
    private final WorkflowAdmissioner workflowAdmissioner;

    public WorkflowAndLocalityAwareEnsembleScheduler(CloudSimWrapper cloudsim, Environment environment,
            RuntimePredictioner runtimePredictioner, WorkflowAdmissioner workflowAdmissioner) {
        super(cloudsim, environment);
        this.runtimePredictioner = runtimePredictioner;
        this.workflowAdmissioner = workflowAdmissioner;
    }

    /**
     * Schedules jobs while minimizing the number of file transfers between them.
     */
    protected boolean scheduleJobsWithTheSamePriority(List<Job> jobs, WorkflowEngine engine) {
        while (!jobs.isEmpty()) {
            List<VM> freeVms = engine.getFreeVMs();
            if (freeVms.isEmpty()) {
                return false;
            }

            Iterator<Job> it = jobs.iterator();
            while (it.hasNext()) {
                if (!workflowAdmissioner.isJobDagAdmitted(it.next(), engine)) {
                    it.remove();
                }
            }

            Job bestJob = null;
            VM bestVM2 = null;
            Double bestSpeedup = null;
            for (Job job : jobs) {
                VM bestLocalVM = freeVms.get(0);
                double bestFinishTime = runtimePredictioner.getPredictedRuntime(job.getTask(), bestLocalVM);
                for (VM vm : freeVms) {
                    double estimatedJobFinish = runtimePredictioner.getPredictedRuntime(job.getTask(), vm);
                    if (estimatedJobFinish <= bestFinishTime) {
                        bestLocalVM = vm;
                        bestFinishTime = estimatedJobFinish;
                    }
                }
                List<VM> allVms = engine.getAvailableVMs();
                for (VM vm : allVms) {
                    if (!vm.isTerminated() && !vm.isFree()) {
                        double t = vm.getPredictedReleaseTime(environment);
                        double estimatedJobFinish = runtimePredictioner.getPredictedRuntime(job.getTask(), vm) + t;
                        if (estimatedJobFinish < bestFinishTime) {
                            bestLocalVM = vm;
                            bestFinishTime = estimatedJobFinish;
                        }
                    }
                }
                double speedup = runtimePredictioner.getPredictedRuntime(job.getTask(), null) - bestFinishTime;
                if (bestSpeedup == null || speedup > bestSpeedup) {
                    bestSpeedup = speedup;
                    bestJob = job;
                    bestVM2 = bestLocalVM;
                }
            }
            if (bestVM2 != null) {
                bestVM2.jobSubmit(bestJob);
            }
            jobs.remove(bestJob);
        }
        return true;
    }

    private final TreeMap<Integer, List<Job>> releasedJobs = new TreeMap<Integer, List<Job>>();

    @Override
    public void scheduleJobs(WorkflowEngine engine) {
        // check the deadline constraints (provisioner takes care about budget)
        double deadline = engine.getDeadline();
        double time = getCloudsim().clock();

        // stop scheduling any new jobs if we are over deadline
        if (time >= deadline) {
            return;
        }

        List<Job> jobs = engine.getAndClearReleasedJobs();
        for (Job job : jobs) {
            Integer key = job.getDAGJob().getPriority();
            if (!releasedJobs.containsKey(key)) {
                releasedJobs.put(key, new LinkedList<Job>());
            }
            releasedJobs.get(key).add(job);
        }

        for (List<Job> jobsAtPriority : releasedJobs.values()) {
            if (!scheduleJobsWithTheSamePriority(jobsAtPriority, engine)) {
                break;
            }
        }

        Iterator<Entry<Integer, List<Job>>> it = releasedJobs.entrySet().iterator();
        while (it.hasNext()) {
            if (it.next().getValue().isEmpty()) {
                it.remove();
            }
        }
    }
}
