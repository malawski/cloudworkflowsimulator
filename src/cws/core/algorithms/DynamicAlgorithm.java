package cws.core.algorithms;

import java.util.HashSet;
import java.util.List;

import cws.core.*;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.core.VMType;
import cws.core.core.VMTypeBuilder;
import cws.core.dag.DAG;
import cws.core.engine.Environment;
import cws.core.provisioner.CloudAwareProvisioner;
import cws.core.provisioner.VMFactory;

public class DynamicAlgorithm extends Algorithm {
    private double price;
    private Scheduler scheduler;
    private CloudAwareProvisioner provisioner;

    public DynamicAlgorithm(double budget, double deadline, List<DAG> dags, double price, Scheduler scheduler,
            CloudAwareProvisioner provisioner, Environment environment, AlgorithmStatistics ensembleStatistics,
            CloudSimWrapper cloudsim) {
        super(budget, deadline, dags, environment, ensembleStatistics, cloudsim);
        this.price = price;
        this.provisioner = provisioner;
        this.scheduler = scheduler;
    }

    @Override
    public void simulateInternal() {
        prepareEnvironment();
        getCloudsim().startSimulation();
    }

    private void prepareEnvironment() {
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
        // Calculate estimated number of VMs to consume budget evenly before deadline
        // ceiling is used to start more vms so that the budget is consumed just before deadline
        // TODO(bryk): Check this, because it doesn't look very right.
        int numEstimatedVMs = (int) Math.ceil(Math.floor(getBudget()) / Math.ceil((getDeadline() / (60 * 60))) / price);

        // Check if we can afford at least one VM
        if (getBudget() < price)
            numEstimatedVMs = 0;
        return numEstimatedVMs;
    }

    private void launchVMs(Cloud cloud, WorkflowEngine engine, int numEstimatedVMs) {
        HashSet<VM> vms = new HashSet<VM>();
        for (int i = 0; i < numEstimatedVMs; i++) {
            VMType vmType = new VMTypeBuilder().mips(1000).cores(1).price(price).build();

            VM vm = VMFactory.createVM(vmType, getCloudsim());
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
