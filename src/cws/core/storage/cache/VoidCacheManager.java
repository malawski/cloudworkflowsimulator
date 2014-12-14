package cws.core.storage.cache;

import cws.core.VM;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.dag.DAGFile;

/**
 * A VMCacheManager that does nothing - it always says that there is no file in the cache. <br>
 * It is a stubby implementation that can be used in tests as well (instead of mocking).
 * 
 * @see {@link VMCacheManager}
 */
public class VoidCacheManager extends VMCacheManager {
    public VoidCacheManager(CloudSimWrapper cloudsim) {
        super(cloudsim);
    }

    @Override
    public void putFileToCache(DAGFile file, VM vm) {
        // Do nothing
    }

    @Override
    public boolean getFileFromCache(DAGFile file, VM vm) {
        // There's no file in the cache, so let's return false.
        return false;
    }
}
