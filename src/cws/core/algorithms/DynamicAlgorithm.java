package cws.core.algorithms;

import java.util.HashSet;
import java.util.List;

import cws.core.AlgorithmStatistics;
import cws.core.Cloud;
import cws.core.EnsembleManager;
import cws.core.Scheduler;
import cws.core.VM;
import cws.core.VMStaticParams;
import cws.core.WorkflowEngine;
import cws.core.WorkflowEvent;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.dag.DAG;
import cws.core.log.WorkflowLog;
import cws.core.provisioner.CloudAwareProvisioner;
import cws.core.provisioner.VMFactory;
import cws.core.storage.StorageManager;

public class DynamicAlgorithm extends Algorithm {
    private double price;
    private Scheduler scheduler;
    private CloudAwareProvisioner provisioner;

    public DynamicAlgorithm(double budget, double deadline, List<DAG> dags, double price, Scheduler scheduler,
            CloudAwareProvisioner provisioner, StorageManager storageManager, AlgorithmStatistics ensembleStatistics,
            CloudSimWrapper cloudsim) {
        super(budget, deadline, dags, storageManager, ensembleStatistics, cloudsim);
        this.price = price;
        this.provisioner = provisioner;
        this.scheduler = scheduler;
    }

    @Override
    public void simulateInternal(String logname) {
        WorkflowLog workflowLog = prepareEnvironment();

        cloudsim.startSimulation();

        printLogs(logname, workflowLog);
    }

    private WorkflowLog prepareEnvironment() {
        setCloud(new Cloud(cloudsim));
        provisioner.setCloud(getCloud());

        setWorkflowEngine(new WorkflowEngine(provisioner, scheduler, cloudsim));
        getWorkflowEngine().setDeadline(getDeadline());
        getWorkflowEngine().setBudget(getBudget());

        scheduler.setStorageManager(storageManager);

        setEnsembleManager(new EnsembleManager(getAllDags(), getWorkflowEngine(), cloudsim));

        WorkflowLog log = initializeLogger(getCloud(), getWorkflowEngine(), getEnsembleManager());

        int estimatedNumVMs = estimateVMsNumber();

        printEstimations(estimatedNumVMs);

        launchVMs(getCloud(), getWorkflowEngine(), estimatedNumVMs);

        return log;
    }

    private WorkflowLog initializeLogger(Cloud cloud, WorkflowEngine engine, EnsembleManager em) {
        WorkflowLog log = null;
        if (shouldGenerateLog()) {
            log = new WorkflowLog(cloudsim);
            engine.addJobListener(log);
            cloud.addVMListener(log);
            em.addDAGJobListener(log);
        }
        return log;
    }

    private int estimateVMsNumber() {
        // Calculate estimated number of VMs to consume budget evenly before deadline
        // ceiling is used to start more vms so that the budget is consumed just before deadline
        // TODO(bryk): Check this.
        int numEstimatedVMs = (int) Math.ceil(Math.floor(getBudget()) / Math.ceil((getDeadline() / (60 * 60))) / price);

        // Check if we can afford at least one VM
        if (getBudget() < price)
            numEstimatedVMs = 0;
        return numEstimatedVMs;
    }

    private void launchVMs(Cloud cloud, WorkflowEngine engine, int numEstimatedVMs) {
        HashSet<VM> vms = new HashSet<VM>();
        for (int i = 0; i < numEstimatedVMs; i++) {
            VMStaticParams vmStaticParams = VMStaticParams.getDefaults();
            vmStaticParams.setPrice(price);

            VM vm = VMFactory.createVM(vmStaticParams, cloudsim);
            vms.add(vm);
            cloudsim.send(engine.getId(), cloud.getId(), 0.0, WorkflowEvent.VM_LAUNCH, vm);
        }
    }

    private void printEstimations(int numVMs) {
        cloudsim.log("Estimated num of VMs " + numVMs);
        cloudsim.log("Total budget " + getBudget());
    }

    private void printLogs(String logname, WorkflowLog workflowLog) {
        if (shouldGenerateLog()) {
            workflowLog.printJobs(logname);
            workflowLog.printVmList(logname);
            workflowLog.printDAGJobs();
        }
        // TODO(bryk): Move this.
        cloudsim.log("Actual cost: " + algorithmStatistics.getActualCost());
        cloudsim.log("Last DAG finished at: " + algorithmStatistics.getActualDagFinishTime());
        cloudsim.log("Last time VM terminated at: " + algorithmStatistics.getActualVMFinishTime());
    }

    @Override
    public long getPlanningnWallTime() {
        // planning is always 0 for dynamic algorithms
        return 0;
    }
}
