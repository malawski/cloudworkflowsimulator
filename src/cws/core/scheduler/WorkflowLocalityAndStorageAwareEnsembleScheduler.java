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
public class WorkflowLocalityAndStorageAwareEnsembleScheduler extends WorkflowAndStorageAwareEnsembleScheduler {
    private final VMCacheManager cacheManager;
    private final StorageManager storageManager;

    public WorkflowLocalityAndStorageAwareEnsembleScheduler(CloudSimWrapper cloudsim, Environment environment) {
        super(cloudsim, environment);
        this.cacheManager = (VMCacheManager) cloudsim.getEntityByName("VMCacheManager");
        this.storageManager = (StorageManager) cloudsim.getEntityByName("StorageManager");
    }

    /**
     * Schedules jobs while minimizing the number of file transfers between them.
     */
    @Override
    protected void scheduleQueue(Queue<Job> jobs, WorkflowEngine engine) {
        while (!jobs.isEmpty() && !engine.getFreeVMs().isEmpty()) {
            Job job = jobs.poll();
            if (isJobDagAdmitted(job, engine)) {
                List<VM> freeVms = engine.getFreeVMs();
                VM bestVM = freeVms.get(0);
                double bestFinishTime = getPredictedRuntime(job.getTask(), bestVM);
                for (VM vm : freeVms) {
                    double estimatedJobFinish = getPredictedRuntime(job.getTask(), vm);
                    if (estimatedJobFinish <= bestFinishTime) {
                        bestVM = vm;
                        bestFinishTime = estimatedJobFinish;
                    }
                }
                List<VM> allVms = engine.getAvailableVMs();
                for (VM vm : allVms) {
                    if (!vm.isFree()) {
                        double t = vm.getPredictedReleaseTime(storageManager, environment, cacheManager);
                        double estimatedJobFinish = getPredictedRuntime(job.getTask(), vm) + t;
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
