package cws.core.algorithms;

import java.util.List;

import cws.core.Cloud;
import cws.core.EnsembleManager;
import cws.core.WorkflowEngine;
import cws.core.cloudsim.CWSSimEntity;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.dag.DAG;
import cws.core.engine.Environment;
import cws.core.log.WorkflowLog;

public abstract class Algorithm extends CWSSimEntity {
    /** Environment of simulation (VMs, storage info) */
    protected Environment environment;

    /** Provides statistics about this algorithm's run */
    protected AlgorithmStatistics algorithmStatistics;

    /** Engine that executes workflows */
    private WorkflowEngine engine;

    /** Ensemble manager that submits DAGs */
    private EnsembleManager manager;

    /** Cloud to provision VMs from */
    private Cloud cloud;

    /** WorkflowLog instance. Logs some interesting data to a file. */
    WorkflowLog workflowLog = new WorkflowLog(getCloudsim());

    /** Should we dump WorkflowLog's logs? */
    private boolean shouldGenerateWorkflowLogs = true; // TODO(bryk): Parameterize this.

    /** Simulation's budget */
    private double budget;

    /** Simulation's deadline */
    private double deadline;

    /** All simulation's DAGs */
    private List<DAG> dags;

    public Algorithm(double budget, double deadline, List<DAG> dags, AlgorithmStatistics algorithmStatistics,
            CloudSimWrapper cloudsim) {
        super("Algorithm", cloudsim);
        this.budget = budget;
        this.deadline = deadline;
        this.dags = dags;
        this.algorithmStatistics = algorithmStatistics;
    }

    /** Should run actual simulation */
    abstract protected void simulateInternal();

    /** Should return the number of wall time nanos spent for planning */
    abstract public long getPlanningnWallTime();

    public void simulate() {
        simulateInternal();
        if (shouldGenerateWorkflowLogs) {
            printWorkflowLogs();
        }
        conductSanityChecks();
    }

    private void printWorkflowLogs() {
        workflowLog.printJobs();
        workflowLog.printVmList();
        workflowLog.printDAGJobs();
    }

    public void setCloud(Cloud cloud) {
        this.cloud = cloud;
        this.cloud.addVMListener(algorithmStatistics);
        this.cloud.addVMListener(workflowLog);
    }

    public void setWorkflowEngine(WorkflowEngine workflowEngine) {
        this.engine = workflowEngine;
        this.engine.addJobListener(algorithmStatistics);
        this.engine.addJobListener(workflowLog);

    }

    public void setEnvironment(Environment environment) {
        this.environment = environment;
    }

    public void setEnsembleManager(EnsembleManager ensembleManager) {
        this.manager = ensembleManager;
        this.manager.addDAGJobListener(algorithmStatistics);
        this.manager.addDAGJobListener(workflowLog);

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

    @Override
    public String getName() {
        return this.getClass().getSimpleName();
    }

    public void setGenerateLog(boolean generateLog) {
        this.shouldGenerateWorkflowLogs = generateLog;
    }
}
