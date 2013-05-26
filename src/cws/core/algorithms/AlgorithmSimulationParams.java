package cws.core.algorithms;

import cws.core.storage.global.GlobalStorageParams;

public class AlgorithmSimulationParams {
    private StorageType storageType;
    private GlobalStorageParams storageParams;
    private StorageCacheType storageCacheType;

    public AlgorithmSimulationParams(StorageType storageType, GlobalStorageParams storageParams,
            StorageCacheType storageCacheType) {
        this.storageType = storageType;
        this.storageParams = storageParams;
        this.storageCacheType = storageCacheType;
    }

    public AlgorithmSimulationParams() {
    }

    public StorageCacheType getStorageCacheType() {
        return storageCacheType;
    }

    public GlobalStorageParams getStorageParams() {
        return storageParams;
    }

    public StorageType getStorageType() {
        return storageType;
    }

    public void setStorageType(StorageType storageType) {
        this.storageType = storageType;
    }

    public void setStorageParams(GlobalStorageParams storageParams) {
        this.storageParams = storageParams;
    }

    public void setStorageCacheType(StorageCacheType storageCacheType) {
        this.storageCacheType = storageCacheType;
    }
}
