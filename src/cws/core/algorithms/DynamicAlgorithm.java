package cws.core.algorithms;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;

import cws.core.Cloud;
import cws.core.EnsembleManager;
import cws.core.Provisioner;
import cws.core.Scheduler;
import cws.core.VM;
import cws.core.VMListener;
import cws.core.WorkflowEngine;
import cws.core.WorkflowEvent;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.dag.DAG;
import cws.core.dag.DAGJob;
import cws.core.dag.DAGJobListener;
import cws.core.experiment.VMFactory;
import cws.core.jobs.Job;
import cws.core.jobs.JobListener;
import cws.core.jobs.SimpleJobFactory;
import cws.core.jobs.Job.Result;
import cws.core.log.WorkflowLog;
import cws.core.storage.StorageManager;
import cws.core.storage.VoidStorageManager;
import cws.core.storage.cache.VMCacheManager;
import cws.core.storage.global.GlobalStorageManager;

public class DynamicAlgorithm extends Algorithm implements DAGJobListener, VMListener, JobListener {

    private double price;

    private Scheduler scheduler;
    private Provisioner provisioner;

    private CloudSimWrapper cloudsim;

    private List<DAG> completedDAGs = new LinkedList<DAG>();

    private double actualCost = 0.0;

    private double actualDagFinishTime = 0.0;
    private double actualVMFinishTime = 0.0;
    private double actualJobFinishTime = 0.0;

    protected long simulationStartWallTime;
    protected long simulationFinishWallTime;

    public DynamicAlgorithm(double budget, double deadline, List<DAG> dags, double price, Scheduler scheduler,
            Provisioner provisioner, CloudSimWrapper cloudsim, StorageManager storageManager) {
        super(budget, deadline, dags, storageManager);
        this.price = price;
        this.provisioner = provisioner;
        this.scheduler = scheduler;
        this.cloudsim = cloudsim;
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
        // TODO(bryk): @mequrel you are re-initing cloudsim here, remember...
        cloudsim.init();

        if (storageManager instanceof GlobalStorageManager) {
            // XXX(bryk): I can't believe I'm writing this code...
            VMCacheManager cacheManager = ((GlobalStorageManager) storageManager).getCacheManager();
            VMCacheManager clone = null;
            try {
                clone = cacheManager.getClass().getConstructor(CloudSimWrapper.class).newInstance(cloudsim);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            storageManager = new GlobalStorageManager(((GlobalStorageManager) storageManager).getParams(), clone,
                    cloudsim);
        } else {
            storageManager = new VoidStorageManager(cloudsim);
        }

        // cloudsim.addEntity(storageManager);

        Cloud cloud = new Cloud(cloudsim);
        cloud.addVMListener(this);
        provisioner.setCloud(cloud);

        WorkflowEngine engine = new WorkflowEngine(new SimpleJobFactory(1000), provisioner, scheduler, cloudsim);
        engine.setDeadline(getDeadline());
        engine.setBudget(getBudget());

        engine.addJobListener(this);

        scheduler.setWorkflowEngine(engine);

        EnsembleManager em = new EnsembleManager(getDAGs(), engine, cloudsim);
        em.addDAGJobListener(this);

        WorkflowLog log = null;
        if (shouldGenerateLog()) {
            log = new WorkflowLog(cloudsim);
            engine.addJobListener(log);
            cloud.addVMListener(log);
            em.addDAGJobListener(log);
        }

        // Calculate estimated number of VMs to consume budget evenly before deadline
        // ceiling is used to start more vms so that the budget is consumed just before deadline
        int numVMs = (int) Math.ceil(Math.floor(getBudget()) / Math.ceil((getDeadline() / (60 * 60))) / price);

        // Check if we can afford at least one VM
        if (getBudget() < price)
            numVMs = 0;

        cloudsim.log(" Estimated num of VMs " + numVMs);
        cloudsim.log(" Total budget " + getBudget());

        // Launch VMs
        HashSet<VM> vms = new HashSet<VM>();
        for (int i = 0; i < numVMs; i++) {
            VM vm = VMFactory.createVM(1000, 1, 1.0, price, cloudsim);
            vms.add(vm);
            cloudsim.send(engine.getId(), cloud.getId(), 0.0, WorkflowEvent.VM_LAUNCH, vm);
        }

        simulationStartWallTime = System.nanoTime();

        cloudsim.startSimulation();

        simulationFinishWallTime = System.nanoTime();

        if (shouldGenerateLog()) {
            log.printJobs(logname);
            log.printVmList(logname);
            log.printDAGJobs();
        }

        cloudsim.log(" Estimated num of VMs " + numVMs);
        cloudsim.log(" Total budget " + getBudget());
        cloudsim.log(" Total cost " + engine.getCost());

        // Set results
        actualCost = engine.getCost();

        for (DAGJob dj : engine.getAllDags()) {
            if (dj.isFinished()) {
                completedDAGs.add(dj.getDAG());
            }
        }

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
}
