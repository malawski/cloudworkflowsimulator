package cws.core.algorithms;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

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

    /**
     * TODO(bryk):
     * @param properties
     */
    public void storeProperties(Properties properties) {
        properties.setProperty("storageType", storageType.toString());
        properties.setProperty("storageCacheType", storageCacheType.toString());
        if (storageParams != null) {
            storageParams.storeProperties(properties);
        }
    }

    /**
     * TODO(bryk): Document it.
     * @return
     */
    public String getName() {
        String ret = "-" + storageType + "_" + storageCacheType;
        if (storageParams != null) {
            ret += storageParams.getName();
        }
        return ret;
    }

    public static List<AlgorithmSimulationParams> getAllSimulationParams() {
        List<AlgorithmSimulationParams> ret = new ArrayList<AlgorithmSimulationParams>();
        AlgorithmSimulationParams voidAll = new AlgorithmSimulationParams(StorageType.VOID, null, StorageCacheType.VOID);
        ret.add(voidAll);
        for (GlobalStorageParams gstorageParam : GlobalStorageParams.getAllGlobalStorageParams()) {
            for (StorageCacheType cache : StorageCacheType.values()) {
                AlgorithmSimulationParams gstorage = new AlgorithmSimulationParams(StorageType.GLOBAL, gstorageParam,
                        cache);
                ret.add(gstorage);
            }
        }
        return ret;
    }

    /**
     * TODO(bryk):
     * @param properties
     * @return
     */
    public static AlgorithmSimulationParams readProperties(Properties properties) {
        AlgorithmSimulationParams params = new AlgorithmSimulationParams();
        if (properties.getProperty("storageType") != null)
            params.storageType = StorageType.valueOf(properties.getProperty("storageType"));
        if (properties.getProperty("storageCacheType") != null)
            params.storageCacheType = StorageCacheType.valueOf(properties.getProperty("storageCacheType"));
        if (params.storageType == StorageType.GLOBAL) {
            params.storageParams = GlobalStorageParams.readProperties(properties);
        }
        return params;
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
