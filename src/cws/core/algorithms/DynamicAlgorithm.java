package cws.core.algorithms;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;

import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.core.CloudSim;

import cws.core.Cloud;
import cws.core.DAGJob;
import cws.core.DAGJobListener;
import cws.core.EnsembleManager;
import cws.core.Job;
import cws.core.JobListener;
import cws.core.Provisioner;
import cws.core.Scheduler;
import cws.core.SimpleJobFactory;
import cws.core.VM;
import cws.core.VMListener;
import cws.core.WorkflowEngine;
import cws.core.WorkflowEvent;
import cws.core.Job.Result;
import cws.core.dag.DAG;
import cws.core.experiment.VMFactory;
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
    
    public DynamicAlgorithm(double budget, double deadline, List<DAG> dags, double price, Scheduler scheduler, Provisioner provisioner) {
        super(budget, deadline, dags);
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
        actualDagFinishTime = Math.max(actualDagFinishTime, CloudSim.clock());
    }

    @Override
    public double getActualDagFinishTime() {
        return actualDagFinishTime;
    }
    
    @Override
    public List<DAG> getCompletedDAGs() {
        return completedDAGs;
    }
    
    public void simulate(String logname) {
        CloudSim.init(1, null, false);
        
        Cloud cloud = new Cloud();
        cloud.addVMListener(this);
        provisioner.setCloud(cloud);
        
        WorkflowEngine engine = new WorkflowEngine(
                new SimpleJobFactory(1000), provisioner, scheduler);
        engine.setDeadline(getDeadline());
        engine.setBudget(getBudget());
        
        engine.addJobListener(this);
        
        scheduler.setWorkflowEngine(engine);
        
        EnsembleManager em = new EnsembleManager(getDAGs(), engine);
        em.addDAGJobListener(this);
        
        WorkflowLog log = null;
        if (shouldGenerateLog()) {
            log = new WorkflowLog();
            engine.addJobListener(log);
            cloud.addVMListener(log);
            em.addDAGJobListener(log);
        }
        
        // Calculate estimated number of VMs to consume budget evenly before deadline
        // ceiling is used to start more vms so that the budget is consumed just before deadline
        int numVMs = (int) Math.ceil(Math.floor(getBudget()) / Math.ceil((getDeadline() / (60 * 60))) / price);
        
        // Check if we can afford at least one VM
        if (getBudget()<price) numVMs = 0;
        
        Log.printLine(CloudSim.clock() + " Estimated num of VMs " + numVMs);
        Log.printLine(CloudSim.clock() + " Total budget " + getBudget());
        
        // Launch VMs
        HashSet<VM> vms = new HashSet<VM>();
        for (int i = 0; i < numVMs; i++) {
            VM vm = VMFactory.createVM(1000, 1, 1.0, price);
            vms.add(vm);
            CloudSim.send(engine.getId(), cloud.getId(), 0.0, WorkflowEvent.VM_LAUNCH, vm);
        }
        
        simulationStartWallTime = System.nanoTime();
        
        CloudSim.startSimulation();
        
        simulationFinishWallTime = System.nanoTime();
        
        if (shouldGenerateLog()) {
            log.printJobs(logname);
            log.printVmList(logname);
            log.printDAGJobs();
        }
        
        Log.printLine(CloudSim.clock() + " Estimated num of VMs " + numVMs);
        Log.printLine(CloudSim.clock() + " Total budget " + getBudget());
        Log.printLine(CloudSim.clock() + " Total cost " + engine.getCost());
        
        // Set results
        actualCost = engine.getCost();
        
        for (DAGJob dj : engine.getAllDags()) {
            if (dj.isFinished()) {
                completedDAGs.add(dj.getDAG());
            }
        }
        
        
        
        if (actualDagFinishTime > getDeadline()) {
            System.err.println("WARNING: Exceeded deadline: "+actualDagFinishTime+">"+getDeadline()+" budget: "+getBudget()+" Estimated num of VMs "+numVMs);
        }
        
        if (getActualCost() > getBudget()) {
            System.err.println("WARNING: Cost exceeded budget: "+getActualCost()+">"+getBudget()+" deadline: "+getDeadline()+" Estimated num of VMs "+numVMs);
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
	public void jobReleased(Job job) {}

	@Override
	public void jobSubmitted(Job job) {}

	@Override
	public void jobStarted(Job job) {}

	@Override
	public void jobFinished(Job job) {
		if (job.getResult() == Result.SUCCESS) {
		    actualJobFinishTime = Math.max(actualJobFinishTime, job.getFinishTime());
		}
	}
}
