package cws.core.storage.cache;

import cws.core.cloudsim.CWSSimEntity;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.dag.DAGFile;
import cws.core.jobs.Job;

/**
 * TODO(bryk): comment it.
 */
public abstract class VMCacheManager extends CWSSimEntity {
    public VMCacheManager(CloudSimWrapper cloudsim) {
        super("VMCacheManager", cloudsim);
    }

    /**
     * TODO(bryk): comment it.
     */
    public abstract void putFileToCache(DAGFile file, Job job);

    /**
     * TODO(bryk): comment it.
     */
    public abstract boolean getFileFromCache(DAGFile file, Job job);
}
