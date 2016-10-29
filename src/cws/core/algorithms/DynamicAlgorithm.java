package cws.core.algorithms;

import java.util.List;

import cws.core.Cloud;
import cws.core.EnsembleManager;
import cws.core.Scheduler;
import cws.core.VM;
import cws.core.VMFactory;
import cws.core.WorkflowEngine;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.core.VMType;
import cws.core.dag.DAG;
import cws.core.engine.Environment;
import cws.core.provisioner.HomogeneousProvisioner;

public class DynamicAlgorithm extends HomogeneousAlgorithm {
    private Scheduler scheduler;

    // Storage for the provisioner until it can be passed to the
    // WorkflowEngine in prepareEnvironment. TODO(david) find a way to
    // remove this.
    private HomogeneousProvisioner tempProvisionerStorage;

    public DynamicAlgorithm(double budget, double deadline, List<DAG> dags, Scheduler scheduler,
            HomogeneousProvisioner provisioner, AlgorithmStatistics ensembleStatistics, Environment environment,
            CloudSimWrapper cloudsim) {
        super(budget, deadline, dags, ensembleStatistics, environment, cloudsim);
        this.tempProvisionerStorage = provisioner;
        this.scheduler = scheduler;
    }

    @Override
    public void simulateInternal() {
        prepareEnvironment();
        getCloudsim().startSimulation();
    }

    private void prepareEnvironment() {
        // TODO(david) this stuff is a nightmare, needs pulling out into a
        // builder class or something.

        Cloud cloud = new Cloud(getCloudsim());

        this.tempProvisionerStorage.setEnvironment(getEnvironment());
        this.tempProvisionerStorage.setCloud(cloud);

        setWorkflowEngine(new WorkflowEngine(tempProvisionerStorage, scheduler, getBudget(), getDeadline(),
                getCloudsim(), getEnvironment()));

        // WorkflowEngine "owns" the provisioner now, so don't use this
        // reference (otherwise we risk it becoming outdated if the
        // provisioner is changed).
        this.tempProvisionerStorage = null;

        setCloud(cloud);

        setEnsembleManager(new EnsembleManager(getAllDags(), getWorkflowEngine(), getCloudsim()));

        int estimatedNumVMs = estimateVMsNumber();

        printEstimations(estimatedNumVMs);

        launchInitialVMs(estimatedNumVMs);
    }

    private int estimateVMsNumber() {
        if (!canAffordAtLeastOneVM()) {
            return 0;
        }

        // TODO vmType should be selected somehow, important!!
        VMType vmType = getEnvironment().getVmTypes().iterator().next();
        return (int) Math.ceil(getMaxSpendingSpeedWeCanAfford() / getEnvironment().getVMTypePrice(vmType));
    }

    private double getMaxSpendingSpeedWeCanAfford() {
        return Math.floor(getBudget())
                / Math.ceil((getDeadline() / getEnvironment().getPricingManager().getBillingTimeInSeconds()));// getBillingTimeInSeconds(getVmType())));
    }

    private boolean canAffordAtLeastOneVM() {
        return getEnvironment().getVMTypePrice(getVmType()) <= getBudget();
    }

    private void launchInitialVMs(int numEstimatedVMs) {
        for (int i = 0; i < numEstimatedVMs; i++) {
            // TODO(mequrel): should be extracted, the best would be to have an interface createVM available
            VM vm = VMFactory.createVM(getVmType(), getCloudsim());
            getProvisioner().launchVM(vm);
        }
    }

    private void printEstimations(int numVMs) {
        getCloudsim().log("Estimated num of VMs " + numVMs);
        getCloudsim().log("Total budget " + getBudget());
    }

    @Override
    public long getPlanningWallTime() {
        // planning is always 0 for dynamic algorithms
        return 0;
    }
}
