package cws.core.simulation;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import cws.core.storage.global.GlobalStorageParams;

/**
 * Storage related parameters for simulations.
 */
public class StorageSimulationParams {
    private StorageType storageType;
    private GlobalStorageParams storageParams;
    private StorageCacheType storageCacheType;

    public StorageSimulationParams(StorageType storageType, GlobalStorageParams storageParams,
            StorageCacheType storageCacheType) {
        this.storageType = storageType;
        this.storageParams = storageParams;
        this.storageCacheType = storageCacheType;
    }

    public StorageSimulationParams() {
    }

    /**
     * Saves this params to the given properties file.
     * @param properties The properties file to save params to.
     */
    public void storeProperties(Properties properties) {
        properties.setProperty("storageType", storageType.toString());
        properties.setProperty("storageCacheType", storageCacheType.toString());
        if (storageParams != null) {
            storageParams.storeProperties(properties);
        }
    }

    /**
     * @return The file name suffix based on this params.
     */
    public String getName() {
        String ret = "-" + storageType + "_" + storageCacheType;
        if (storageParams != null) {
            ret += storageParams.getName();
        }
        return ret;
    }

    /**
     * @return All simulation params permutations.
     */
    public static List<StorageSimulationParams> getAllSimulationParams() {
        List<StorageSimulationParams> ret = new ArrayList<StorageSimulationParams>();
        StorageSimulationParams voidAll = new StorageSimulationParams(StorageType.VOID, null, StorageCacheType.VOID);
        ret.add(voidAll);
        for (GlobalStorageParams gstorageParam : GlobalStorageParams.getAllGlobalStorageParams()) {
            for (StorageCacheType cache : StorageCacheType.values()) {
                StorageSimulationParams gstorage = new StorageSimulationParams(StorageType.GLOBAL, gstorageParam, cache);
                ret.add(gstorage);
            }
        }
        return ret;
    }

    /**
     * Reads simulation params from a properties file.
     * @param properties The properties file to read from.
     * @return Newly created simulation params instance.
     */
    public static StorageSimulationParams readProperties(Properties properties) {
        StorageSimulationParams params = new StorageSimulationParams();
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
