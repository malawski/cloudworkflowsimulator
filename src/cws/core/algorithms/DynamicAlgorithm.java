package cws.core.algorithms;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;

import cws.core.*;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.dag.DAG;
import cws.core.dag.DAGJob;
import cws.core.dag.DAGJobListener;
import cws.core.experiment.VMFactory;
import cws.core.jobs.Job;
import cws.core.jobs.Job.Result;
import cws.core.jobs.JobListener;
import cws.core.jobs.SimpleJobFactory;
import cws.core.log.WorkflowLog;

public class DynamicAlgorithm extends Algorithm implements DAGJobListener, VMListener, JobListener {

    private double price;

    private Scheduler scheduler;
    private Provisioner provisioner;

    private List<DAG> completedDAGs = new LinkedList<DAG>();

    private double actualCost = 0.0;

    private double actualDagFinishTime = 0.0;
    private double actualVMFinishTime = 0.0;
    private double actualJobFinishTime = 0.0;

    protected long simulationStartWallTime;
    protected long simulationFinishWallTime;

    public DynamicAlgorithm(double budget, double deadline, List<DAG> dags, double price, Scheduler scheduler,
            Provisioner provisioner, CloudSimWrapper cloudsim, AlgorithmSimulationParams simulationParams) {
        super(budget, deadline, dags, simulationParams, cloudsim);
        this.price = price;
        this.provisioner = provisioner;
        this.scheduler = scheduler;
    }

    @Override
    public double getActualCost() {
        return actualCost;
    }

    @Override
    public void dagStarted(DAGJob dagJob) {
        /* Do nothing */
    }

    @Override
    public void dagFinished(DAGJob dagJob) {
        actualDagFinishTime = Math.max(actualDagFinishTime, cloudsim.clock());
    }

    @Override
    public double getActualDagFinishTime() {
        return actualDagFinishTime;
    }

    @Override
    public List<DAG> getCompletedDAGs() {
        return completedDAGs;
    }

    @Override
    public void simulate(String logname) {
        // re-initing cloudsim here, watch out...
        cloudsim.init();

        Algorithm.initializeStorage(simulationParams, cloudsim);

        SimulationEnvironment simulationEnvironment = prepareEnvironment();

        simulationStartWallTime = System.nanoTime();

        cloudsim.startSimulation();

        simulationFinishWallTime = System.nanoTime();

        printLogs(logname, simulationEnvironment);

        setResults(simulationEnvironment.getEngine());

        conductSanityChecks(simulationEnvironment.getNumVMs());
    }

    private SimulationEnvironment prepareEnvironment() {
        Cloud cloud = new Cloud(cloudsim);
        cloud.addVMListener(this);
        provisioner.setCloud(cloud);

        WorkflowEngine engine = new WorkflowEngine(new SimpleJobFactory(1000), provisioner, scheduler, cloudsim);
        engine.setDeadline(getDeadline());
        engine.setBudget(getBudget());

        engine.addJobListener(this);

        scheduler.setWorkflowEngine(engine);
        scheduler.setStorageManager(storageManager);

        EnsembleManager em = new EnsembleManager(getDAGs(), engine, cloudsim);
        em.addDAGJobListener(this);

        WorkflowLog log = initializeLogger(cloud, engine, em);

        int numVMs = determineVMsNumber();

        printEstimations(numVMs);

        launchVMs(cloud, engine, numVMs);

        return new SimulationEnvironment(numVMs, log, engine);
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

    private int determineVMsNumber() {
        // Calculate estimated number of VMs to consume budget evenly before deadline
        // ceiling is used to start more vms so that the budget is consumed just before deadline
        int numVMs = (int) Math.ceil(Math.floor(getBudget()) / Math.ceil((getDeadline() / (60 * 60))) / price);

        // Check if we can afford at least one VM
        if (getBudget() < price)
            numVMs = 0;
        return numVMs;
    }

    private void launchVMs(Cloud cloud, WorkflowEngine engine, int numVMs) {
        HashSet<VM> vms = new HashSet<VM>();
        for (int i = 0; i < numVMs; i++) {
            VMStaticParams vmStaticParams = VMStaticParams.getDefaults();
            vmStaticParams.setPrice(price);

            VM vm = VMFactory.createVM(vmStaticParams, cloudsim);
            vms.add(vm);
            cloudsim.send(engine.getId(), cloud.getId(), 0.0, WorkflowEvent.VM_LAUNCH, vm);
        }
    }

    private void printEstimations(int numVMs) {
        cloudsim.log(" Estimated num of VMs " + numVMs);
        cloudsim.log(" Total budget " + getBudget());
    }

    private void printLogs(String logname, SimulationEnvironment environment) {
        WorkflowEngine engine = environment.getEngine();
        WorkflowLog log = environment.getLog();
        int numVMs = environment.getNumVMs();

        if (shouldGenerateLog()) {
            log.printJobs(logname);
            log.printVmList(logname);
            log.printDAGJobs();
        }

        printEstimations(numVMs);
        cloudsim.log(" Total cost " + engine.getCost());
    }

    private void setResults(WorkflowEngine engine) {
        actualCost = engine.getCost();

        for (DAGJob dj : engine.getAllDags()) {
            if (dj.isFinished()) {
                completedDAGs.add(dj.getDAG());
            }
        }
    }

    private void conductSanityChecks(int numVMs) {
        if (actualDagFinishTime > getDeadline()) {
            System.err.println("WARNING: Exceeded deadline: " + actualDagFinishTime + ">" + getDeadline() + " budget: "
                    + getBudget() + " Estimated num of VMs " + numVMs);
        }

        if (getActualCost() > getBudget()) {
            System.err.println("WARNING: Cost exceeded budget: " + getActualCost() + ">" + getBudget() + " deadline: "
                    + getDeadline() + " Estimated num of VMs " + numVMs);
        }
    }

    @Override
    public long getSimulationWallTime() {
        return simulationFinishWallTime - simulationStartWallTime;
    }

    @Override
    public long getPlanningnWallTime() {
        // planning is always 0 for dynamic algorithms
        return 0;
    }

    @Override
    public double getActualJobFinishTime() {
        return actualJobFinishTime;
    }

    @Override
    public double getActualVMFinishTime() {
        return actualVMFinishTime;
    }

    @Override
    public void vmLaunched(VM vm) {

    }

    @Override
    public void vmTerminated(VM vm) {
        actualVMFinishTime = Math.max(actualVMFinishTime, vm.getTerminateTime());
    }

    @Override
    public void jobReleased(Job job) {
    }

    @Override
    public void jobSubmitted(Job job) {
    }

    @Override
    public void jobStarted(Job job) {
    }

    @Override
    public void jobFinished(Job job) {
        if (job.getResult() == Result.SUCCESS) {
            actualJobFinishTime = Math.max(actualJobFinishTime, job.getFinishTime());
        }
    }

    private class SimulationEnvironment {
        private WorkflowEngine engine;
        private WorkflowLog log;
        private int numVMs;

        public SimulationEnvironment(int numVMs, WorkflowLog log, WorkflowEngine engine) {
            this.numVMs = numVMs;
            this.log = log;
            this.engine = engine;
        }

        public WorkflowEngine getEngine() {
            return engine;
        }

        public WorkflowLog getLog() {
            return log;
        }

        public int getNumVMs() {
            return numVMs;
        }

    }
}
