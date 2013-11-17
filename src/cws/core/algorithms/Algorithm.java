package cws.core.algorithms;

import java.util.List;

import cws.core.AlgorithmStatistics;
import cws.core.Cloud;
import cws.core.EnsembleManager;
import cws.core.WorkflowEngine;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.dag.DAG;
import cws.core.storage.StorageManager;

public abstract class Algorithm {
    protected CloudSimWrapper cloudsim;
    protected StorageManager storageManager;
    protected AlgorithmStatistics algorithmStatistics;
    /** Engine that executes workflows */
    private WorkflowEngine engine;

    /** Ensemble manager that submits DAGs */
    private EnsembleManager manager;

    /** Cloud to provision VMs from */
    private Cloud cloud;

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

    public void simulate(String logname) {
        simulateInternal(logname);
        conductSanityChecks();
    }

    public void setCloud(Cloud cloud) {
        this.cloud = cloud;
        this.cloud.addVMListener(algorithmStatistics);
    }

    public void setWorkflowEngine(WorkflowEngine workflowEngine) {
        this.engine = workflowEngine;
        this.engine.addJobListener(algorithmStatistics);
    }

    public void setEnsembleManager(EnsembleManager ensembleManager) {
        this.manager = ensembleManager;
        this.manager.addDAGJobListener(algorithmStatistics);
    }

    private void conductSanityChecks() {
        if (algorithmStatistics.getActualDagFinishTime() > getDeadline()) {
            System.err.println("WARNING: Exceeded deadline: " + algorithmStatistics.getActualDagFinishTime() + ">"
                    + getDeadline() + " budget: " + getBudget());
        }

        if (algorithmStatistics.getActualCost() > getBudget()) {
            System.err.println("WARNING: Cost exceeded budget: " + algorithmStatistics.getActualCost() + ">"
                    + getBudget() + " deadline: " + getDeadline());
        }
    }

    abstract protected void simulateInternal(String logname);

    abstract public long getPlanningnWallTime();

    public EnsembleManager getEnsembleManager() {
        return manager;
    }

    public WorkflowEngine getWorkflowEngine() {
        return engine;
    }

    public Cloud getCloud() {
        return cloud;
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

    protected CloudSimWrapper getCloudsim() {
        return cloudsim;
    }

    public StorageManager getStorageManager() {
        return storageManager;
    }
}
