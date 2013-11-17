package cws.core.algorithms;

import java.util.List;

import cws.core.AlgorithmStatistics;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.dag.DAG;
import cws.core.storage.StorageManager;
import cws.core.storage.VoidStorageManager;
import cws.core.storage.cache.FIFOCacheManager;
import cws.core.storage.cache.VMCacheManager;
import cws.core.storage.cache.VoidCacheManager;
import cws.core.storage.global.GlobalStorageManager;

public abstract class Algorithm {
    /** Simulation params like storage manager type, needed to initialize simulation properly. **/
    protected StorageSimulationParams simulationParams;
    protected CloudSimWrapper cloudsim;
    protected StorageManager storageManager;
    protected AlgorithmStatistics algorithmStatistics;

    private double budget;
    private double deadline;
    private List<DAG> dags;
    private boolean generateLog = false;

    public Algorithm(double budget, double deadline, List<DAG> dags, StorageSimulationParams simulationParams,
            AlgorithmStatistics algorithmStatistics, CloudSimWrapper cloudsim) {
        this.budget = budget;
        this.deadline = deadline;
        this.dags = dags;
        this.simulationParams = simulationParams;
        this.cloudsim = cloudsim;
        this.algorithmStatistics = algorithmStatistics;
    }

    public AlgorithmStatistics getAlgorithmStatistics() {
        return this.algorithmStatistics;
    }

    public List<DAG> getAllDags() {
        return dags;
    }

    public double getBudget() {
        return budget;
    }

    public double getDeadline() {
        return deadline;
    }

    public String getName() {
        return this.getClass().getSimpleName();
    }

    public boolean shouldGenerateLog() {
        return this.generateLog;
    }

    public void setGenerateLog(boolean generateLog) {
        this.generateLog = generateLog;
    }

    abstract public void simulate(String logname);

    abstract public long getPlanningnWallTime();

    public static StorageManager initializeStorage(StorageSimulationParams simulationParams, CloudSimWrapper cloudsim) {
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

    protected CloudSimWrapper getCloudsim() {
        return cloudsim;
    }

    public StorageManager getStorageManager() {
        return storageManager;
    }
}
