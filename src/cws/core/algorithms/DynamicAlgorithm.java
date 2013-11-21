package cws.core.algorithms;

import java.util.HashSet;
import java.util.List;

import cws.core.*;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.dag.DAG;
import cws.core.provisioner.CloudAwareProvisioner;
import cws.core.provisioner.VMFactory;

public class DynamicAlgorithm extends Algorithm {
    private Scheduler scheduler;
    private CloudAwareProvisioner provisioner;

    public DynamicAlgorithm(double budget, double deadline, List<DAG> dags, Scheduler scheduler,
            CloudAwareProvisioner provisioner, AlgorithmStatistics ensembleStatistics, CloudSimWrapper cloudsim) {
        super(budget, deadline, dags, ensembleStatistics, cloudsim);
        this.provisioner = provisioner;
        this.scheduler = scheduler;
    }

    @Override
    public void simulateInternal() {
        prepareEnvironment();
        getCloudsim().startSimulation();
    }

    private void prepareEnvironment() {
        provisioner.setEnvironment(environment);
        scheduler.setEnvironment(environment);

        setCloud(new Cloud(getCloudsim()));
        provisioner.setCloud(getCloud());

        setWorkflowEngine(new WorkflowEngine(provisioner, scheduler, getCloudsim()));
        getWorkflowEngine().setDeadline(getDeadline());
        getWorkflowEngine().setBudget(getBudget());

        setEnsembleManager(new EnsembleManager(getAllDags(), getWorkflowEngine(), getCloudsim()));

        int estimatedNumVMs = estimateVMsNumber();

        printEstimations(estimatedNumVMs);

        launchVMs(getCloud(), getWorkflowEngine(), estimatedNumVMs);
    }

    private int estimateVMsNumber() {
        if (!canAffordAtLeastOneVM()) {
            return 0;
        }

        return (int) Math.ceil(getMaxSpendingSpeedWeCanAfford() / environment.getSingleVMPrice());
    }

    private double getMaxSpendingSpeedWeCanAfford() {
        return Math.floor(getBudget()) / Math.ceil((getDeadline() / environment.getBillingTimeInSeconds()));
    }

    private boolean canAffordAtLeastOneVM() {
        return environment.getSingleVMPrice() <= getBudget();
    }

    private void launchVMs(Cloud cloud, WorkflowEngine engine, int numEstimatedVMs) {
        HashSet<VM> vms = new HashSet<VM>();
        for (int i = 0; i < numEstimatedVMs; i++) {
            // TODO(mequrel): should be extracted, the best would be to have an interface createVM available
            VM vm = VMFactory.createVM(environment.getVMType(), getCloudsim());
            vms.add(vm);
            getCloudsim().send(engine.getId(), cloud.getId(), 0.0, WorkflowEvent.VM_LAUNCH, vm);
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
