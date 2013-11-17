package cws.core.algorithms;

import java.util.List;

import cws.core.AlgorithmStatistics;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.dag.DAG;
import cws.core.storage.StorageManager;

public abstract class Algorithm {
    protected CloudSimWrapper cloudsim;
    protected StorageManager storageManager;
    protected AlgorithmStatistics algorithmStatistics;

    private double budget;
    private double deadline;
    private List<DAG> dags;
    private boolean generateLog = false;

    public Algorithm(double budget, double deadline, List<DAG> dags, StorageManager storageManager,
            AlgorithmStatistics algorithmStatistics, CloudSimWrapper cloudsim) {
        this.budget = budget;
        this.deadline = deadline;
        this.dags = dags;
        this.cloudsim = cloudsim;
        this.algorithmStatistics = algorithmStatistics;
        this.storageManager = storageManager;
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

    protected CloudSimWrapper getCloudsim() {
        return cloudsim;
    }

    public StorageManager getStorageManager() {
        return storageManager;
    }
}
