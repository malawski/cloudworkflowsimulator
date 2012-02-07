package cws.core.provisioner;

import static org.junit.Assert.*;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.cloudbus.cloudsim.core.CloudSim;
import org.junit.Test;

import cws.core.Cloud;
import cws.core.EnsembleManager;
import cws.core.Provisioner;
import cws.core.Scheduler;
import cws.core.SimpleJobFactory;
import cws.core.VM;
import cws.core.WorkflowEngine;
import cws.core.WorkflowEvent;
import cws.core.dag.DAG;
import cws.core.dag.DAGParser;
import cws.core.experiment.Experiment;
import cws.core.experiment.ExperimentDescription;
import cws.core.experiment.ExperimentResult;
import cws.core.log.WorkflowLog;
import cws.core.scheduler.DAGDynamicScheduler;
import cws.core.scheduler.EnsembleDynamicScheduler;
import cws.core.scheduler.WorkflowAwareEnsembleScheduler;


public class DynamicProvisionerDynamicSchedulerTest implements WorkflowEvent {
	private String dagPath = "dags/";
	
	@Test
	public void testProvisionerScheduleDag100x10() {
		CloudSim.init(1, null, false);
		
		Cloud cloud = new Cloud();
		
		SimpleQueueBasedProvisioner provisioner = new SimpleQueueBasedProvisioner();
		provisioner.setCloud(cloud);
		
		DAGDynamicScheduler scheduler = new EnsembleDynamicScheduler();
		
		WorkflowEngine engine = new WorkflowEngine(new SimpleJobFactory(1000), provisioner, scheduler);
		
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
		
		new EnsembleManager(dags, engine);

		
		CloudSim.startSimulation();

		assertEquals(0, engine.getQueuedJobs().size());
		
		wfLog.printJobs("testEnsembleProvisionerDynamicSchedulerDag_CyberShake_100x10");
		wfLog.printVmList("testEnsembleProvisionerDynamicSchedulerDag_CyberShake_100x10");
	}
	
	
	@Test
	public void testDPDS() {
		String dagName = "CyberShake_1000.dag";
		double deadline = 7200.0; //seconds
		double[] budgets = {49.0, 48.0, 45.0, 44.0, 40.0, 10.0, 9.0, 8.0, 7.0, 6.0, 5.0, 4.0, 1.0};
		double price = 1.0;
		int numDAGs = 40;
		double max_scaling = 2.0;
		
		
		for (double budget : budgets) {
		    runWAExperiment(dagPath, dagName, deadline, budget, price, numDAGs, max_scaling);
		    runNormalExperiment(dagPath, dagName, deadline, budget, price, numDAGs, max_scaling);
		}
	}
	
	
	@Test
	public void testAwareDPDS() {
		String dagName = "CyberShake_1000.dag";
		double deadline = 7200.0; //seconds
		double[] budgets = {41.0, 0.5, 1.5, 2.5, 11.3, 12.7};
		double price = 1.0;
		int numDAGs = 40;
		double max_scaling = 2.0;
		
		for (double budget : budgets){
		    runWAExperiment(dagPath, dagName, deadline, budget, price, numDAGs, max_scaling);
		    runNormalExperiment(dagPath, dagName, deadline, budget, price, numDAGs, max_scaling);
		}
	}
	
	@Test
	public void testAwareDPDSMontage() {
	    
		String dagName = "Montage_1000.dag";
		
		double deadline = 7200.0; //seconds
		double budget = 73.0;
		double price = 1.0;
		int numDAGs = 40;
		double max_scaling = 2.0;
		
		runWAExperiment(dagPath, dagName, deadline, budget, price, numDAGs, max_scaling);
		runNormalExperiment(dagPath, dagName, deadline, budget, price, numDAGs, max_scaling);
	}
	
	@Test
	public void testAwareDPDSInspiral() {
		
		String dagName = "Inspiral_1000.dag";
		
		double deadline = 72000.0; //seconds
		double budget = 400.0;
		double price = 1.0;
		int numDAGs = 40;
		double max_scaling = 2.0;
		
		runWAExperiment(dagPath, dagName, deadline, budget, price, numDAGs, max_scaling);
		runNormalExperiment(dagPath, dagName, deadline, budget, price, numDAGs, max_scaling);
	}

	@Test
	public void testAwareDPDSEpigenomics() {
		
		String dagName = "GENOME.n.1000.0.dag";
		
		double price = 1.0;
		int numDAGs = 40;
		double max_scaling = 2.0;
		
		{
		    double deadline = 720000.0;
		    double budget = 3350.0;
		    runWAExperiment(dagPath, dagName, deadline, budget, price, numDAGs, max_scaling);
		    runNormalExperiment(dagPath, dagName, deadline, budget, price, numDAGs, max_scaling);
		}
		{
		    double deadline = 115200.0;
		    double budget = 35000.0;
		    runWAExperiment(dagPath, dagName, deadline, budget, price, numDAGs, max_scaling);
		    runNormalExperiment(dagPath, dagName, deadline, budget, price, numDAGs, max_scaling);
		}
	}
	
	@Test
	public void testAwareDPDSAvianflu() {
		
		String dagName = "avianflu_large.dag";
		
		double deadline = 720000.0; //seconds
		double price = 1.0;
		int numDAGs = 40;
		
		{
		    double max_scaling = 2.0;
		    double budget = 20000.0;
		    runWAExperiment(dagPath, dagName, deadline, budget, price, numDAGs, max_scaling);
		    runNormalExperiment(dagPath, dagName, deadline, budget, price, numDAGs, max_scaling);
		}
		{
		    double max_scaling = 0.0;
		    double budget = 20000.0;
		    runWAExperiment(dagPath, dagName, deadline, budget, price, numDAGs, max_scaling);
		    runNormalExperiment(dagPath, dagName, deadline, budget, price, numDAGs, max_scaling);
		}
	}
	
	@Test
	public void testAwareDPDSPsload_large() {
		
		String dagName = "psload_large.dag";
		
		double deadline = 72000.0; //seconds
		double budget = 800.0;
		double price = 1.0;
		int numDAGs = 40;
		
		{
		    double max_scaling = 2.0;
		    runWAExperiment(dagPath, dagName, deadline, budget, price, numDAGs, max_scaling);
		    runNormalExperiment(dagPath, dagName, deadline, budget, price, numDAGs, max_scaling);
		}
		{
		    double max_scaling = 0.0;
		    runWAExperiment(dagPath,  dagName, deadline, budget, price, numDAGs, max_scaling);
		    runNormalExperiment(dagPath, dagName, deadline, budget, price, numDAGs, max_scaling);
		}
	}
	
	@Test
	public void testAwareDPDSPsmerge_small() {
		
		String dagName = "psmerge_small.dag";
		
		double deadline = 72000.0; //seconds
		double budget = 8000.0;
		double price = 1.0;
		int numDAGs = 40;
		{
		    double max_scaling = 2.0;
		    runWAExperiment(dagPath, dagName, deadline, budget, price, numDAGs, max_scaling);
		    runNormalExperiment(dagPath, dagName, deadline, budget, price, numDAGs, max_scaling);
		}
		{
		    double max_scaling = 0.0;
		    runWAExperiment(dagPath, dagName, deadline, budget, price, numDAGs, max_scaling);
		    runNormalExperiment(dagPath, dagName, deadline, budget, price, numDAGs, max_scaling);
		}
	}
	
	@Test
	public void testMaxScaling() {
		
		String dagName = "GENOME.n.1000.0.dag";
		
		double price = 1.0;
		int numDAGs = 40;
		
		{
		    double max_scaling = 2.0;
		    {
		        double budget = 3350.0;
		        double deadline = 72000.0;
		        runWAExperiment(dagPath, dagName, deadline, budget, price, numDAGs, max_scaling);
		        runNormalExperiment(dagPath, dagName, deadline, budget, price, numDAGs, max_scaling);
		    }
		    {
		        double budget = 35000.0;
		        double deadline = 115200.0;
		        runWAExperiment(dagPath, dagName, deadline, budget, price, numDAGs, max_scaling);
		        runNormalExperiment(dagPath, dagName, deadline, budget, price, numDAGs, max_scaling);
		    }
		}
		{
		    double max_scaling = 0;
		    {
		        double budget = 3350.0;
		        double deadline = 72000.0;
		        runWAExperiment(dagPath, dagName, deadline, budget, price, numDAGs, max_scaling);
		        runNormalExperiment(dagPath, dagName, deadline, budget, price, numDAGs, max_scaling);
		    }
		    {
		        double budget = 35000.0;
		        double deadline = 115200.0;
		        runWAExperiment(dagPath, dagName, deadline, budget, price, numDAGs, max_scaling);
		        runNormalExperiment(dagPath, dagName, deadline, budget, price, numDAGs, max_scaling);
		    }
		}
	}
	
	@Test
	public void testAwareDPDSSipht() {
		String dagName = "Sipht_1000.dag";
		double deadline = 200000.0; //seconds
		double budget = 457.0;
		double price = 1.0;
		int numDAGs = 40;
		double max_scaling = 2.0;
		
		runWAExperiment(dagPath, dagName, deadline, budget, price, numDAGs, max_scaling);
		runNormalExperiment(dagPath, dagName, deadline, budget, price, numDAGs, max_scaling);
	}

	@Test
	public void testBudgetOverrun() {
		String dagName = "Sipht_1000.dag";
		double deadline = 200000.0; //seconds
		double budget = 450.0;
		double price = 1.0;
		int numDAGs = 40;
		double max_scaling = 2.0;
		
		runNormalExperiment(dagPath, dagName, deadline, budget, price, numDAGs, max_scaling);
	}
	
	private void runWAExperiment(String dagPath, String dagName, double deadline, double budget, double price, int numDAGs, double max_scaling) {

		String dags[] = new String[numDAGs];
		for (int i=0; i< numDAGs; i++) dags[i] = dagName;
		
        ExperimentDescription param = new ExperimentDescription("WADPDS", dagPath, dags,
                deadline, budget, price, max_scaling, 0.7);
        
        runTestExperiment(param);
    }
	
	private void runNormalExperiment(String dagPath, String dagName, double deadline, double budget, double price, int numDAGs, double max_scaling) {

		String dags[] = new String[numDAGs];
		for (int i=0; i< numDAGs; i++) dags[i] = dagName;
		
        ExperimentDescription param = new ExperimentDescription("DPDS", dagPath, dags,
                deadline, budget, price, max_scaling, 0.7);
        
        runTestExperiment(param);
	}
	
	public void runTestExperiment(ExperimentDescription param) {
		Experiment experiment = new Experiment();
		ExperimentResult result = experiment.runExperiment(param);
		
		assertTrue(result.getCost()<=result.getBudget());
		assertTrue(result.getNumBusyVMs()==0);
		assertTrue(result.getNumFreeVMs()==0);
	}
}
