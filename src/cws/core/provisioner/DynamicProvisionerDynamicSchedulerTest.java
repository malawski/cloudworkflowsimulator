package cws.core.provisioner;

import static org.junit.Assert.*;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.core.CloudSim;
import org.junit.Test;

import cws.core.Cloud;
import cws.core.EnsembleManager;
import cws.core.Provisioner;
import cws.core.Scheduler;
import cws.core.VM;
import cws.core.WorkflowEngine;
import cws.core.WorkflowEvent;
import cws.core.dag.DAG;
import cws.core.dag.DAGParser;
import cws.core.log.WorkflowLog;
import cws.core.scheduler.DAGDynamicScheduler;
import cws.core.scheduler.EnsembleDynamicScheduler;
import cws.core.scheduler.WorkflowAwareEnsembleScheduler;


public class DynamicProvisionerDynamicSchedulerTest implements WorkflowEvent {

	
	
	@Test
	public void testProvisionerScheduleDag100x10() {
		
		
		CloudSim.init(1, null, false);

		
		Provisioner provisioner = new SimpleQueueBasedProvisioner();
		DAGDynamicScheduler scheduler = new EnsembleDynamicScheduler();
		WorkflowEngine engine = new WorkflowEngine(provisioner , scheduler);
		Cloud cloud = new Cloud();
		provisioner.setCloud(cloud);


		WorkflowLog wfLog = new WorkflowLog();		
		engine.addJobListener(wfLog);
		cloud.addVMListener(wfLog);
		
		engine.setDeadline(7200.0);
		engine.setBudget(45.0);
		
		HashSet<VM> vms = new HashSet<VM>();
		for (int i = 0; i < 10; i++) {
			VM vm = new VM(1000, 1, 1.0, 1.0);
			vms.add(vm);
			CloudSim.send(engine.getId(), cloud.getId(), 0.0, VM_LAUNCH, vm);
		}
		
		List<DAG> dags = new ArrayList<DAG>();

		for (int i = 0; i < 10; i++) {
			DAG dag = DAGParser.parseDAG(new File("dags/CyberShake_100.dag"));
			dags.add(dag);
		}
		
		EnsembleManager em = new EnsembleManager(dags, engine);

		
		CloudSim.startSimulation();

		assertEquals(0, engine.getQueuedJobs().size());
		
		wfLog.printJobs("testEnsembleProvisionerDynamicSchedulerDag_CyberShake_100x10");
		wfLog.printVmList("testEnsembleProvisionerDynamicSchedulerDag_CyberShake_100x10");
	}
	
	
	@Test
	public void testDPDS() {
		
		double deadline = 7200.0; //seconds
		double budget;
		double price = 1.0;
		int numDAGs = 40;
		
		budget = 49.0;
		runScenario(new SimpleUtilizationBasedProvisioner(), new EnsembleDynamicScheduler(), deadline, budget, price, numDAGs);
		
		budget = 48.0;
		runScenario(new SimpleUtilizationBasedProvisioner(), new EnsembleDynamicScheduler(), deadline, budget, price, numDAGs);
		
		budget = 45.0;
		runScenario(new SimpleUtilizationBasedProvisioner(), new EnsembleDynamicScheduler(), deadline, budget, price, numDAGs);
		
		budget = 44.0;
		runScenario(new SimpleUtilizationBasedProvisioner(), new EnsembleDynamicScheduler(), deadline, budget, price, numDAGs);
		
		budget = 40.0;
		runScenario(new SimpleUtilizationBasedProvisioner(), new EnsembleDynamicScheduler(), deadline, budget, price, numDAGs);

		budget = 10.0;
		runScenario(new SimpleUtilizationBasedProvisioner(), new EnsembleDynamicScheduler(), deadline, budget, price, numDAGs);

		budget = 9.0;
		runScenario(new SimpleUtilizationBasedProvisioner(), new EnsembleDynamicScheduler(), deadline, budget, price, numDAGs);

		budget = 8.0;
		runScenario(new SimpleUtilizationBasedProvisioner(), new EnsembleDynamicScheduler(), deadline, budget, price, numDAGs);

		budget = 7.0;
		runScenario(new SimpleUtilizationBasedProvisioner(), new EnsembleDynamicScheduler(), deadline, budget, price, numDAGs);

		budget = 6.0;
		runScenario(new SimpleUtilizationBasedProvisioner(), new EnsembleDynamicScheduler(), deadline, budget, price, numDAGs);

		budget = 5.0;
		runScenario(new SimpleUtilizationBasedProvisioner(), new EnsembleDynamicScheduler(), deadline, budget, price, numDAGs);
		
		budget = 4.0;
		runScenario(new SimpleUtilizationBasedProvisioner(), new EnsembleDynamicScheduler(), deadline, budget, price, numDAGs);
		
		budget = 1.0;
		runScenario(new SimpleUtilizationBasedProvisioner(), new EnsembleDynamicScheduler(), deadline, budget, price, numDAGs);
		
	}
	

	
	@Test
	public void testAwareDPDS() {
		
		double deadline = 7200.0; //seconds
		double budget;
		double price = 1.0;
		int numDAGs = 40;
		
		
		budget = 49.0;
		runScenario(new SimpleUtilizationBasedProvisioner(), new WorkflowAwareEnsembleScheduler(), deadline, budget, price, numDAGs);
		runScenario(new SimpleUtilizationBasedProvisioner(), new EnsembleDynamicScheduler(), deadline, budget, price, numDAGs);
		
		// for this budget we should see the improvement of aware over unaware algorithm
		budget = 48.0;
		runScenario(new SimpleUtilizationBasedProvisioner(), new WorkflowAwareEnsembleScheduler(), deadline, budget, price, numDAGs);
		runScenario(new SimpleUtilizationBasedProvisioner(), new EnsembleDynamicScheduler(), deadline, budget, price, numDAGs);
		
	}
	
	
	public void runScenario(Provisioner provisioner, Scheduler scheduler, double deadline, double budget, double price, int numDAGs) {
			
		CloudSim.init(1, null, false);

			
		List<DAG> dags = new ArrayList<DAG>();
		
		String dagName = "CyberShake_1000.dag";


		
		WorkflowEngine engine = new WorkflowEngine(provisioner , scheduler);
		Cloud cloud = new Cloud();
		provisioner.setCloud(cloud);
		
		for (int i = 0; i < numDAGs; i++) {
			DAG dag = DAGParser.parseDAG(new File("dags/" + dagName));
			dags.add(dag);
		}			
		EnsembleManager em = new EnsembleManager(dags, engine);

		engine.setDeadline(deadline);
		engine.setBudget(budget);
		
		WorkflowLog wfLog = new WorkflowLog();		
		engine.addJobListener(wfLog);
		cloud.addVMListener(wfLog);
		em.addDAGJobListener(wfLog);
		

		
		int numVMs = (int) Math.floor(budget / (deadline / (60 * 60)) / price); 
		Log.printLine(CloudSim.clock() + " Estimated num of VMs " + numVMs);
		Log.printLine(CloudSim.clock() + " Total budget " + budget);

		
		HashSet<VM> vms = new HashSet<VM>();
		for (int i = 0; i < numVMs; i++) {
			VM vm = new VM(1000, 1, 1.0, price);
			vms.add(vm);			
			CloudSim.send(engine.getId(), cloud.getId(), 0.0, VM_LAUNCH, vm);
		}	
		
		CloudSim.startSimulation();
		
		assertTrue(engine.getCost()<=engine.getBudget());
		assertTrue(engine.getBusyVMs().isEmpty());
		assertTrue(engine.getBusyVMs().isEmpty());
		
		Log.printLine(CloudSim.clock() + " Estimated num of VMs " + numVMs);
		Log.printLine(CloudSim.clock() + " Total budget " + budget);
		Log.printLine(CloudSim.clock() + " Total cost " + engine.getCost());

		
		String fName = "test" + provisioner.getClass().getSimpleName()+scheduler.getClass().getSimpleName()+dagName+"x"+numDAGs+"d"+deadline+"b"+budget;
		
		wfLog.printJobs(fName);
		wfLog.printVmList(fName);
		wfLog.printDAGJobs();
		

		
	}
	

}
