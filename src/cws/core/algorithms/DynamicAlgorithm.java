package cws.core.algorithms;

import java.util.HashSet;
import java.util.List;

import cws.core.Cloud;
import cws.core.EnsembleManager;
import cws.core.AlgorithmStatistics;
import cws.core.Provisioner;
import cws.core.Scheduler;
import cws.core.VM;
import cws.core.VMStaticParams;
import cws.core.WorkflowEngine;
import cws.core.WorkflowEvent;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.dag.DAG;
import cws.core.jobs.SimpleJobFactory;
import cws.core.log.WorkflowLog;
import cws.core.provisioner.VMFactory;
import cws.core.storage.StorageManager;

public class DynamicAlgorithm extends Algorithm {
    private double price;
    private Scheduler scheduler;
    private Provisioner provisioner;

    public DynamicAlgorithm(double budget, double deadline, List<DAG> dags, double price, Scheduler scheduler,
            Provisioner provisioner, StorageManager storageManager, AlgorithmStatistics ensembleStatistics,
            CloudSimWrapper cloudsim) {
        super(budget, deadline, dags, storageManager, ensembleStatistics, cloudsim);
        this.price = price;
        this.provisioner = provisioner;
        this.scheduler = scheduler;
    }

    @Override
    public void simulate(String logname) {
        SimulationEnvironment simulationEnvironment = prepareEnvironment();

        cloudsim.startSimulation();

        printLogs(logname, simulationEnvironment);

        conductSanityChecks(simulationEnvironment.getEstimatedNumVMs());
    }

    private SimulationEnvironment prepareEnvironment() {
        Cloud cloud = new Cloud(cloudsim);
        cloud.addVMListener(algorithmStatistics);
        provisioner.setCloud(cloud);

        WorkflowEngine engine = new WorkflowEngine(new SimpleJobFactory(1000), provisioner, scheduler, cloudsim);
        engine.setDeadline(getDeadline());
        engine.setBudget(getBudget());

        engine.addJobListener(algorithmStatistics);

        scheduler.setWorkflowEngine(engine);
        scheduler.setStorageManager(storageManager);

        EnsembleManager em = new EnsembleManager(getAllDags(), engine, cloudsim);
        em.addDAGJobListener(algorithmStatistics);

        WorkflowLog log = initializeLogger(cloud, engine, em);

        int estimatedNumVMs = estimateVMsNumber();

        printEstimations(estimatedNumVMs);

        launchVMs(cloud, engine, estimatedNumVMs);

        return new SimulationEnvironment(estimatedNumVMs, log);
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

    private void printLogs(String logname, SimulationEnvironment environment) {
        WorkflowLog log = environment.getLog();

        if (shouldGenerateLog()) {
            log.printJobs(logname);
            log.printVmList(logname);
            log.printDAGJobs();
        }
        // TODO(bryk): Move this.
        cloudsim.log("Actual cost: " + algorithmStatistics.getActualCost());
        cloudsim.log("Last DAG finished at: " + algorithmStatistics.getActualDagFinishTime());
        cloudsim.log("Last time VM terminated at: " + algorithmStatistics.getActualVMFinishTime());
    }

    private void conductSanityChecks(int numVMs) {
        // TODO(bryk): Move this.
        if (algorithmStatistics.getActualDagFinishTime() > getDeadline()) {
            System.err.println("WARNING: Exceeded deadline: " + algorithmStatistics.getActualDagFinishTime() + ">"
                    + getDeadline() + " budget: " + getBudget() + " Estimated num of VMs " + numVMs);
        }

        if (algorithmStatistics.getActualCost() > getBudget()) {
            System.err.println("WARNING: Cost exceeded budget: " + algorithmStatistics.getActualCost() + ">"
                    + getBudget() + " deadline: " + getDeadline() + " Estimated num of VMs " + numVMs);
        }
    }

    @Override
    public long getPlanningnWallTime() {
        // planning is always 0 for dynamic algorithms
        return 0;
    }

    private class SimulationEnvironment {
        private WorkflowLog log;
        private int estimatedNumVMs;

        public SimulationEnvironment(int numVMs, WorkflowLog log) {
            this.estimatedNumVMs = numVMs;
            this.log = log;
        }

        public WorkflowLog getLog() {
            return log;
        }

        public int getEstimatedNumVMs() {
            return estimatedNumVMs;
        }
    }
}
