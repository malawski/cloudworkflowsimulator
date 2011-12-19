package cws.core.experiment;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.core.CloudSim;

import cws.core.Cloud;
import cws.core.DAGJob;
import cws.core.EnsembleManager;
import cws.core.SimpleJobFactory;
import cws.core.VM;
import cws.core.WorkflowEngine;
import cws.core.WorkflowEvent;
import cws.core.dag.DAG;
import cws.core.dag.DAGParser;
import cws.core.log.WorkflowLog;
import cws.core.provisioner.SimpleUtilizationBasedProvisioner;
import cws.core.scheduler.EnsembleDynamicScheduler;
import cws.core.scheduler.WorkflowAwareEnsembleScheduler;

public class Experiment {
	
	/**
	 * Runs experiment given its description
	 * @param param experiment description
	 * @return results
	 */
	
	public ExperimentResult runExperiment(ExperimentDescription param) {
		
		ExperimentResult result = new ExperimentResult();
			
		CloudSim.init(1, null, false);
	
		List<DAG> dags = new ArrayList<DAG>();
		
		WorkflowEngine engine = new WorkflowEngine(new SimpleJobFactory(1000), param.getProvisioner(), param.getScheduler());
		Cloud cloud = new Cloud();
		param.getProvisioner().setCloud(cloud);
		param.getProvisioner().setMax_scaling(param.getMax_scaling());
		
		for (int i = 0; i < param.getDags().length; i++) {
			DAG dag = parse(new File(param.getDagPath() + param.getDags()[i]));
			dags.add(dag);
		}		
		
		EnsembleManager em = new EnsembleManager(dags, engine);

		engine.setDeadline(param.getDeadline());
		engine.setBudget(param.getBudget());
		
		WorkflowLog wfLog = new WorkflowLog();		
		engine.addJobListener(wfLog);
		cloud.addVMListener(wfLog);
		em.addDAGJobListener(wfLog);
		

		// calculate estimated number of VMs to consume budget evenly before deadline
		// ceiling is used to start more vms so that the budget is consumed just before deadline
		int numVMs = (int) Math.ceil(param.getBudget() / (param.getDeadline() / (60 * 60)) / param.getPrice()); 
		Log.printLine(CloudSim.clock() + " Estimated num of VMs " + numVMs);
		Log.printLine(CloudSim.clock() + " Total budget " + param.getBudget());

		
		HashSet<VM> vms = new HashSet<VM>();
		for (int i = 0; i < numVMs; i++) {
			VM vm = new VM(1000, 1, 1.0, param.getPrice());
			vms.add(vm);			
			CloudSim.send(engine.getId(), cloud.getId(), 0.0, WorkflowEvent.VM_LAUNCH, vm);
		}	
		
		CloudSim.startSimulation();
		
		String fName = "test" + param.getProvisioner().getClass().getSimpleName()+param.getScheduler().getClass().getSimpleName()+param.getDags()[0]+"x"+param.getDags().length+"d"+param.getDeadline()+"b"+param.getBudget()+"m"+param.getMax_scaling();
		
		wfLog.printJobs(fName);
		wfLog.printVmList(fName);
		wfLog.printDAGJobs();		
		
		Log.printLine(CloudSim.clock() + " Estimated num of VMs " + numVMs);
		Log.printLine(CloudSim.clock() + " Total budget " + param.getBudget());
		Log.printLine(CloudSim.clock() + " Total cost " + engine.getCost());
		
		result.setBudget(param.getBudget());
		result.setCost(engine.getCost());
		result.setNumBusyVMs(engine.getBusyVMs().size());
		result.setNumFreeVMs(engine.getFreeVMs().size());
		result.setDeadline(engine.getDeadline());
	
		
		int finished = 0;
		for (DAGJob dj : engine.getAllDags()) {
			if (dj.isFinished()) finished++;
		}
		result.setNumFinishedDAGs(finished);
		
		return result;
	}
	
	
	/**
	 * Helper method to read DAX or DAG dile format. 
	 * XML-based DAX seems to be 10x slower.
	 * @param file
	 * @return DAG object
	 */
	
	private DAG parse (File file) {
		if (file.getName().endsWith("dag")) return DAGParser.parseDAG(file);
		else if (file.getName().endsWith("dax")) return DAGParser.parseDAX(file);
		else throw new RuntimeException("Unrecognized file: " + file.getName());		
	}
	
	
	/**
	 * Runs a series of experiments for varying deadlines
	 * Uses two schedulers: workflow aware and unaware.
	 * For each scheduler a text file is produced, containing number of workflows finished for a given deadline.
	 * 
	 * @param dagPath path to dags
	 * @param dags array of file names
	 * @param budget budget in $
	 * @param price VM hour price in $
	 * @param N max deadline in hours
	 * @param step step between deadlines in hours
	 * @param start min deadline in hours
	 * @param max_scaling max autoscaling factor
	 * @param runID id of this series
	 */
	
	public static void runSeries(String dagPath, String[] dags, double budget, double price,
			int N, int step, int start, double max_scaling, int runID) {
		
		double deadline;
		ExperimentResult resultsAware[] = new ExperimentResult[N+1];
		ExperimentResult resultsSimple[] = new ExperimentResult[N+1];

		
		for (int i=start; i<= N; i+=step) {
			deadline = 3600*i; //seconds
			Experiment experiment = new Experiment();
			resultsAware[i] = experiment.runExperiment(new ExperimentDescription(new SimpleUtilizationBasedProvisioner(), new WorkflowAwareEnsembleScheduler(), dagPath, dags,
				deadline, budget, price, max_scaling));
			resultsSimple[i] = experiment.runExperiment(new ExperimentDescription(new SimpleUtilizationBasedProvisioner(), new EnsembleDynamicScheduler(), dagPath, dags,
					deadline, budget, price, max_scaling));
		}
		
		StringBuffer outAware = new StringBuffer();
		StringBuffer outSimple = new StringBuffer();

		for (int i=start; i<= N; i+=step) {
			outAware.append(resultsAware[i].getDeadline() + "  " + resultsAware[i].getNumFinishedDAGs() + "\n");
			outSimple.append(resultsSimple[i].getDeadline() + "  " + resultsSimple[i].getNumFinishedDAGs() + "\n");
			
		}
		
		WorkflowLog.stringToFile(outAware.toString(), dags[0] + "b" + budget + "h" +start + "-" + N + "m" + max_scaling + "run" + runID + "-outputAware.txt");
		WorkflowLog.stringToFile(outSimple.toString(), dags[0] + "b" + budget + "h" +start + "-" + N + "m" + max_scaling + "run" + runID + "-outputSimple.txt");			
		
	}
	
	/** 
	 * 
	 * Rus a series of experiments, sets runID = 0
	 * 
	 * @param dagPath
	 * @param dags
	 * @param budget
	 * @param price
	 * @param N
	 * @param step
	 * @param start
	 * @param max_scaling
	 */
	
	public static void runSeries(String dagPath, String[] dags, double budget, double price,
			 int N, int step, int start, double max_scaling) {
		
		runSeries(dagPath, dags, budget, price, N, step, start, max_scaling, 0);
	}
	
	
	/** 
	 * 
	 * Runs a series constructing dags array by repeating the same DAG file numDAGs times.
	 * 
	 * @param dagPath
	 * @param dagName
	 * @param budget
	 * @param price
	 * @param numDAGs
	 * @param N
	 * @param step
	 * @param start
	 * @param max_scaling
	 * @param runID
	 */
	
	public static void runSeries(String dagPath, String dagName, double budget, double price,
			int numDAGs, int N, int step, int start, double max_scaling, int runID) {
		
		String[] dags = new String[numDAGs];
		
		for (int i=0; i< numDAGs; i++) dags[i] = dagName;
		
		runSeries(dagPath, dags, budget, price, N, step, start, max_scaling, runID);

	}
	
	/**
	 * Runs a series constructing dags array by repeating the same DAG file numDAGs times.
	 * Sets runID to 0.
	 * 
	 * @param dagPath
	 * @param dagName
	 * @param budget
	 * @param price
	 * @param numDAGs
	 * @param N
	 * @param step
	 * @param start
	 * @param max_scaling
	 */
	
	public static void runSeries(String dagPath, String dagName, double budget, double price,
			int numDAGs, int N, int step, int start, double max_scaling) {
		runSeries(dagPath, dagName, budget, price, numDAGs, N, step, start, max_scaling, 0);
	}
	
	/**
	 * Repeats a series runs times, increasing runID from 0 to runs-1
	 * @param runs number of runs
	 */
	
	public static void runSeriesRepeat(String dagPath, String dagName, double budget, double price,
			int numDAGs, int N, int step, int start, double max_scaling, int runs) {
		
		for (int i=0; i< runs; i++) {
			runSeries(dagPath, dagName, budget, price, numDAGs, N, step, start, max_scaling, i);
		}	
	}


	/**
	 * Repeats a series runs times, increasing runID from 0 to runs-1
	 * @param runs number of runs
	 */
	
	public static void runSeriesRepeat(String dagPath, String[] dags,
			double budget, double price, int N, int step, int start,
			double max_scaling, int runs) {

		for (int i=0; i< runs; i++) {
			runSeries(dagPath, dags, budget, price, N, step, start, max_scaling, i);
		}
	}

}
