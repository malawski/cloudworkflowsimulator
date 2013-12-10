package cws.core.storage;

import cws.core.cloudsim.CloudSimWrapper;
import cws.core.simulation.StorageCacheType;
import cws.core.simulation.StorageSimulationParams;
import cws.core.simulation.StorageType;
import cws.core.storage.cache.FIFOCacheManager;
import cws.core.storage.cache.VMCacheManager;
import cws.core.storage.cache.VoidCacheManager;
import cws.core.storage.global.GlobalStorageManager;

public class StorageManagerFactory {

    public static StorageManager createStorage(StorageSimulationParams simulationParams, CloudSimWrapper cloudsim) {
        VMCacheManager cacheManager;
        if (simulationParams.getStorageCacheType() == StorageCacheType.FIFO) {
            cacheManager = new FIFOCacheManager(cloudsim);
        } else {
            cacheManager = new VoidCacheManager(cloudsim);
        }
        StorageManager storageManager;
        if (simulationParams.getStorageType() == StorageType.GLOBAL) {
            storageManager = new GlobalStorageManager(simulationParams.getStorageParams(), cacheManager, cloudsim);
        } else {
            storageManager = new VoidStorageManager(cloudsim);
        }
        return storageManager;
    }
}
