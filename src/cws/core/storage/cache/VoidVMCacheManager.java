package cws.core.storage.cache;

import cws.core.cloudsim.CloudSimWrapper;
import cws.core.dag.DAGFile;
import cws.core.jobs.Job;

/**
 * A VMCacheManager that does nothing - it always says that there is no file in the cache. <br>
 * It is a stubby implementation that can be used in tests as well (instead of mocking).
 * 
 * @see {@link VMCacheManager}
 */
public class VoidVMCacheManager extends VMCacheManager {
    public VoidVMCacheManager(CloudSimWrapper cloudsim) {
        super(cloudsim);
    }

    @Override
    public void putFileToCache(DAGFile file, Job job) {
        // Do nothing
    }

    @Override
    public boolean getFileFromCache(DAGFile file, Job job) {
        // There's no file in the cache, so let's return false.
        return false;
    }
}
