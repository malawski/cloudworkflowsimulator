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
import cws.core.Provisioner;
import cws.core.Scheduler;
import cws.core.SimpleJobFactory;
import cws.core.VM;
import cws.core.WorkflowEngine;
import cws.core.WorkflowEvent;
import cws.core.dag.DAG;
import cws.core.experiment.VMFactory;
import cws.core.log.WorkflowLog;

public class DynamicAlgorithm extends Algorithm implements DAGJobListener {
    
    private double price;
    
    private Scheduler scheduler;
    
    private Provisioner provisioner;
    
    private List<DAG> completedDAGs = new LinkedList<DAG>();
    
    private double actualCost = 0.0;
    
    private double actualFinishTime = 0.0;
    
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
        actualFinishTime = Math.max(actualFinishTime, CloudSim.clock());
    }

    @Override
    public double getActualFinishTime() {
        return actualFinishTime;
    }
    
    @Override
    public List<DAG> getCompletedDAGs() {
        return completedDAGs;
    }
    
    public void simulate(String logname) {
        CloudSim.init(1, null, false);
        
        Cloud cloud = new Cloud();
        provisioner.setCloud(cloud);
        
        WorkflowEngine engine = new WorkflowEngine(
                new SimpleJobFactory(1000), provisioner, scheduler);
        engine.setDeadline(getDeadline());
        engine.setBudget(getBudget());
        
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
        
        if (actualFinishTime > getDeadline()) {
            System.err.println("WARNING: Exceeded deadline: "+actualFinishTime+">"+getDeadline());
        }
        
        if (getActualCost() > getBudget()) {
            System.err.println("WARNING: Cost exceeded budget: "+getActualCost()+">"+getBudget() + " deadline: " +getDeadline()+ " Estimated num of VMs " + numVMs);
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
}
