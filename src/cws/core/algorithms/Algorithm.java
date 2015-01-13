package cws.core.algorithms;

import java.util.List;

import cws.core.Cloud;
import cws.core.EnsembleManager;
import cws.core.WorkflowEngine;
import cws.core.Provisioner;
import cws.core.provisioner.CloudAwareProvisioner;
import cws.core.cloudsim.CWSSimEntity;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.dag.DAG;
import cws.core.log.WorkflowLog;

public abstract class Algorithm extends CWSSimEntity {

    /** Provides statistics about this algorithm's run */
    protected AlgorithmStatistics algorithmStatistics;

    /** Engine that executes workflows */
    private WorkflowEngine engine;

    /** Ensemble manager that submits DAGs */
    private EnsembleManager manager;

    /** Provisioner to decide when to provision/deprovision VMs */
    protected CloudAwareProvisioner provisioner;

    /** Cloud to provision VMs from */
    private Cloud cloud;

    /** WorkflowLog instance. Logs some interesting data to a file. */
    WorkflowLog workflowLog = new WorkflowLog(getCloudsim());

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

    public final void simulate() {
        simulateInternal();
        printWorkflowLogs();
        conductSanityChecks();
    }

    private void printWorkflowLogs() {
        workflowLog.printJobs();
        workflowLog.printVmList();
        workflowLog.printDAGJobs();
    }

    public final void setCloud(Cloud cloud) {
        this.cloud = cloud;
        this.cloud.addVMListener(algorithmStatistics);
        this.cloud.addVMListener(workflowLog);
    }

    public final void setWorkflowEngine(WorkflowEngine workflowEngine) {
        this.engine = workflowEngine;
        this.engine.addJobListener(algorithmStatistics);
        this.engine.addJobListener(workflowLog);

        this.provisioner = workflowEngine.getProvisioner();
    }

    public final void setEnsembleManager(EnsembleManager ensembleManager) {
        this.manager = ensembleManager;
        this.manager.addDAGJobListener(algorithmStatistics);
        this.manager.addDAGJobListener(workflowLog);
    }

    private final void conductSanityChecks() {
        if (algorithmStatistics.getLastDagFinishTime() > getDeadline()) {
            System.err.println("NOTE: Exceeded deadline: " + algorithmStatistics.getLastDagFinishTime() + ">"
                    + getDeadline() + " budget: " + getBudget());
        }

        if (algorithmStatistics.getCost() > getBudget()) {
            System.err.println("NOTE: Cost exceeded budget: " + algorithmStatistics.getCost() + ">" + getBudget()
                    + " deadline: " + getDeadline());
        }
    }

    public final EnsembleManager getEnsembleManager() {
        return manager;
    }

    public final WorkflowEngine getWorkflowEngine() {
        return engine;
    }

    public final CloudAwareProvisioner getProvisioner() {
        return provisioner;
    }

    public final Cloud getCloud() {
        return cloud;
    }

    public final AlgorithmStatistics getAlgorithmStatistics() {
        return this.algorithmStatistics;
    }

    public final List<DAG> getAllDags() {
        return dags;
    }

    public final double getBudget() {
        return budget;
    }

    public final double getDeadline() {
        return deadline;
    }

    @Override
    public final String getName() {
        return this.getClass().getSimpleName();
    }
}
