package cws.core.algorithms;

import java.util.List;

import cws.core.Cloud;
import cws.core.EnsembleManager;
import cws.core.Scheduler;
import cws.core.VM;
import cws.core.VMFactory;
import cws.core.WorkflowEngine;
import cws.core.WorkflowEvent;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.dag.DAG;
import cws.core.engine.Environment;
import cws.core.provisioner.HomogeneousProvisioner;

public class DynamicAlgorithm extends HomogeneousAlgorithm {
    private Scheduler scheduler;
    private HomogeneousProvisioner provisioner;

    public DynamicAlgorithm(double budget, double deadline, List<DAG> dags, Scheduler scheduler,
            HomogeneousProvisioner provisioner, AlgorithmStatistics ensembleStatistics, Environment environment,
            CloudSimWrapper cloudsim) {
        super(budget, deadline, dags, ensembleStatistics, environment, cloudsim);
        this.provisioner = provisioner;
        this.scheduler = scheduler;
    }

    @Override
    public void simulateInternal() {
        prepareEnvironment();
        getCloudsim().startSimulation();
    }

    private void prepareEnvironment() {
        provisioner.setEnvironment(getEnvironment());

        setCloud(new Cloud(getCloudsim()));
        provisioner.setCloud(getCloud());

        setWorkflowEngine(new WorkflowEngine(provisioner, scheduler, getBudget(), getDeadline(), getCloudsim()));

        setEnsembleManager(new EnsembleManager(getAllDags(), getWorkflowEngine(), getCloudsim()));

        int estimatedNumVMs = estimateVMsNumber();

        printEstimations(estimatedNumVMs);

        launchInitialVMs(getCloud(), getWorkflowEngine(), estimatedNumVMs);
    }

    private int estimateVMsNumber() {
        if (!canAffordAtLeastOneVM()) {
            return 0;
        }

        return (int) Math.ceil(getMaxSpendingSpeedWeCanAfford() / getEnvironment().getSingleVMPrice());
    }

    private double getMaxSpendingSpeedWeCanAfford() {
        return Math.floor(getBudget()) / Math.ceil((getDeadline() / getEnvironment().getBillingTimeInSeconds()));
    }

    private boolean canAffordAtLeastOneVM() {
        return getEnvironment().getSingleVMPrice() <= getBudget();
    }

    private void launchInitialVMs(Cloud cloud, WorkflowEngine engine, int numEstimatedVMs) {
        for (int i = 0; i < numEstimatedVMs; i++) {
            // TODO(mequrel): should be extracted, the best would be to have an interface createVM available
            VM vm = VMFactory.createVM(getEnvironment().getVMType(), getCloudsim());
            getProvisioner().launchVM(getWorkflowEngine().getId(), vm);
        }
    }

    private void printEstimations(int numVMs) {
        getCloudsim().log("Estimated num of VMs " + numVMs);
        getCloudsim().log("Total budget " + getBudget());
    }

    @Override
    public long getPlanningnWallTime() {
        // planning is always 0 for dynamic algorithms
        return 0;
    }
}
