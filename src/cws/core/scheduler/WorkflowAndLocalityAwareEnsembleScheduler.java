package cws.core.scheduler;

import java.util.List;
import java.util.Queue;

import cws.core.VM;
import cws.core.WorkflowEngine;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.engine.Environment;
import cws.core.jobs.Job;
import cws.core.storage.StorageManager;
import cws.core.storage.cache.VMCacheManager;

/**
 * {@link WorkflowAwareEnsembleScheduler} implementation that is also aware of the underlying storage and schedules jobs
 * to minimize file transfers.
 */
public class WorkflowAndLocalityAwareEnsembleScheduler extends EnsembleDynamicScheduler {
    private final VMCacheManager cacheManager;
    private final StorageManager storageManager;
    private final RuntimePredictioner runtimePredictioner;
    private final WorkflowAdmissioner workflowAdmissioner;

    public WorkflowAndLocalityAwareEnsembleScheduler(CloudSimWrapper cloudsim, Environment environment,
            RuntimePredictioner runtimePredictioner, WorkflowAdmissioner workflowAdmissioner) {
        super(cloudsim, environment);
        this.cacheManager = (VMCacheManager) cloudsim.getEntityByName("VMCacheManager");
        this.storageManager = (StorageManager) cloudsim.getEntityByName("StorageManager");
        this.runtimePredictioner = runtimePredictioner;
        this.workflowAdmissioner = workflowAdmissioner;
    }

    /**
     * Schedules jobs while minimizing the number of file transfers between them.
     */
    @Override
    protected void scheduleQueue(Queue<Job> jobs, WorkflowEngine engine) {
        while (!jobs.isEmpty() && !engine.getFreeVMs().isEmpty()) {
            Job job = jobs.poll();
            if (workflowAdmissioner.isJobDagAdmitted(job, engine)) {
                List<VM> freeVms = engine.getFreeVMs();
                VM bestVM = freeVms.get(0);
                double bestFinishTime = runtimePredictioner.getPredictedRuntime(job.getTask(), bestVM);
                for (VM vm : freeVms) {
                    double estimatedJobFinish = runtimePredictioner.getPredictedRuntime(job.getTask(), vm);
                    if (estimatedJobFinish <= bestFinishTime) {
                        bestVM = vm;
                        bestFinishTime = estimatedJobFinish;
                    }
                }
                List<VM> allVms = engine.getAvailableVMs();
                for (VM vm : allVms) {
                    if (!vm.isFree()) {
                        double t = vm.getPredictedReleaseTime(storageManager, environment, cacheManager);
                        double estimatedJobFinish = runtimePredictioner.getPredictedRuntime(job.getTask(), vm) + t;
                        if (estimatedJobFinish < bestFinishTime) {
                            bestVM = vm;
                            bestFinishTime = estimatedJobFinish;
                        }
                    }
                }
                bestVM.jobSubmit(job);
            }
        }
    }
}
