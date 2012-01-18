package cws.core.experiment;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;

import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.core.CloudSim;

import cws.core.Cloud;
import cws.core.DAGJob;
import cws.core.EnsembleManager;
import cws.core.Provisioner;
import cws.core.Scheduler;
import cws.core.SimpleJobFactory;
import cws.core.VM;
import cws.core.WorkflowEngine;
import cws.core.WorkflowEvent;
import cws.core.algorithms.SPSS;
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
		
		boolean useSPSS = false;
		
		ExperimentResult result = new ExperimentResult();
		
		Provisioner prov = param.getProvisioner();
		
		Scheduler sched = param.getScheduler();
		
		if (sched.getClass().getName() == "cws.core.algorithms.SPSS") useSPSS = true;
		
		CloudSim.init(1, null, false);

		
		Cloud cloud = new Cloud();
		prov.setCloud(cloud);
		
		WorkflowEngine engine;
		
		if (useSPSS)  engine = new WorkflowEngine(new SimpleJobFactory(1), prov, sched);
		else engine = new WorkflowEngine(new SimpleJobFactory(1000), prov, sched);
		
		sched.setWorkflowEngine(engine);
		
		List<DAG> dags = new ArrayList<DAG>();
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
		
		if (!useSPSS) { 
			HashSet<VM> vms = new HashSet<VM>();
			for (int i = 0; i < numVMs; i++) {
				VM vm = new VM(1000, 1, 1.0, param.getPrice());
				vms.add(vm);
				CloudSim.send(engine.getId(), cloud.getId(), 0.0, WorkflowEvent.VM_LAUNCH, vm);
			}	
		}

		if (useSPSS) {
			((SPSS)sched).setEnsembleManager(em);
			((SPSS)sched).plan();
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
		result.setDeadline(param.getDeadline());
		result.setCost(engine.getCost());
		result.setNumBusyVMs(engine.getBusyVMs().size());
		result.setNumFreeVMs(engine.getFreeVMs().size());
		
		List<Integer> priorities = new LinkedList<Integer>();
		List<Double> sizes = new LinkedList<Double>();

		
		int finished = 0;
		for (DAGJob dj : engine.getAllDags()) {
			if (dj.isFinished()) {
				finished++;
				priorities.add(dj.getPriority());
				sizes.add(sumRuntime(dj.getDAG()));
			}
		}
		result.setNumFinishedDAGs(finished);
		result.setPriorities(priorities);
		result.setSizes(sizes);
		
		return result;
	}
	
	
	/**
	 * @return The total runtime of all tasks in the workflow
	 */
	public double sumRuntime(DAG dag) {
        double sum = 0.0;
        for (String taskName : dag.getTasks()) {
            sum += dag.getTask(taskName).size;
        }
        return sum;
    }
	
	
	/**
	 * Helper method to read DAX or DAG dile format. 
	 * XML-based DAX seems to be 10x slower.
	 * @param file
	 * @return DAG object
	 */
	
	private static DAG parse (File file) {
		if (file.getName().endsWith("dag")) return DAGParser.parseDAG(file);
		else if (file.getName().endsWith("dax")) return DAGParser.parseDAX(file);
		else throw new RuntimeException("Unrecognized file: " + file.getName());
	}
	
	
	/**
	 * Runs a series of experiments for varying deadlines
	 * Uses two schedulers: workflow aware and unaware.
	 * For each scheduler a text file is produced, containing number of workflows finished for a given deadline.
	 * 
	 * @param prefix prefix to append to generated output files
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
	
	public static void runSeries(String prefix, String dagPath, String[] dags, double budget, double price,
			int N, int step, int start, double max_scaling, int runID) {
		
		double deadline;
		ExperimentResult resultsSPSS[] = new ExperimentResult[N+1];
		ExperimentResult resultsAware[] = new ExperimentResult[N+1];
		ExperimentResult resultsSimple[] = new ExperimentResult[N+1];
		
		for (int i=start; i<= N; i+=step) {
			deadline = 3600*i; //seconds
			Experiment experiment = new Experiment();
			
			//FIXME: align interfaces
			List<DAG> dag_objects = new ArrayList<DAG>();
			for (int j = 0; j < dags.length; j++) {
				DAG dag = parse(new File(dagPath + dags[j]));
				dag_objects.add(dag);
			}
			
			SPSS spss = new SPSS(budget, deadline, dag_objects, 0.7);

			resultsSPSS[i] = experiment.runExperiment(new ExperimentDescription(
			        spss, spss, dagPath, dags,
			        deadline, budget, price, max_scaling));
			resultsAware[i] = experiment.runExperiment(new ExperimentDescription(
			        new SimpleUtilizationBasedProvisioner(max_scaling), 
			        new WorkflowAwareEnsembleScheduler(), dagPath, dags,
			        deadline, budget, price, max_scaling));
			resultsSimple[i] = experiment.runExperiment(new ExperimentDescription(
			        new SimpleUtilizationBasedProvisioner(max_scaling), 
			        new EnsembleDynamicScheduler(), dagPath, dags,
			        deadline, budget, price, max_scaling));
		}
		
		// write number of dags finished
		
		StringBuffer outSPSS = new StringBuffer();
		StringBuffer outAware = new StringBuffer();
		StringBuffer outSimple = new StringBuffer();

		for (int i=start; i<= N; i+=step) {
			outSPSS.append(resultsSPSS[i].getDeadline() + "  " + resultsSPSS[i].getNumFinishedDAGs() + "\n");
			outAware.append(resultsAware[i].getDeadline() + "  " + resultsAware[i].getNumFinishedDAGs() + "\n");
			outSimple.append(resultsSimple[i].getDeadline() + "  " + resultsSimple[i].getNumFinishedDAGs() + "\n");
			
		}

		WorkflowLog.stringToFile(outSPSS.toString(), prefix + dags[0] + "b" + budget + "h" +start + "-" + N + "m" + max_scaling + "run" + runID + "-outputSPSS.txt");
		WorkflowLog.stringToFile(outAware.toString(), prefix + dags[0] + "b" + budget + "h" +start + "-" + N + "m" + max_scaling + "run" + runID + "-outputAware.txt");
		WorkflowLog.stringToFile(outSimple.toString(), prefix + dags[0] + "b" + budget + "h" +start + "-" + N + "m" + max_scaling + "run" + runID + "-outputSimple.txt");			

		// write priorities of dags finished
		
		StringBuffer prioritiesSPSS = new StringBuffer();
		StringBuffer prioritiesAware = new StringBuffer();
		StringBuffer prioritiesSimple = new StringBuffer();

		for (int i=start; i<= N; i+=step) {
			prioritiesSPSS.append(resultsSPSS[i].formatPriorities());
			prioritiesAware.append(resultsAware[i].formatPriorities());
			prioritiesSimple.append(resultsSimple[i].formatPriorities());			
		}

		WorkflowLog.stringToFile(prioritiesSPSS.toString(), prefix + dags[0] + "b" + budget + "h" +start + "-" + N + "m" + max_scaling + "run" + runID + "-prioritiesSPSS.txt");
		WorkflowLog.stringToFile(prioritiesAware.toString(), prefix + dags[0] + "b" + budget + "h" +start + "-" + N + "m" + max_scaling + "run" + runID + "-prioritiesAware.txt");
		WorkflowLog.stringToFile(prioritiesSimple.toString(), prefix + dags[0] + "b" + budget + "h" +start + "-" + N + "m" + max_scaling + "run" + runID + "-prioritiesSimple.txt");			

		// write sizes of dags finished

		StringBuffer sizesSPSS = new StringBuffer();
		StringBuffer sizesAware = new StringBuffer();
		StringBuffer sizesSimple = new StringBuffer();

		for (int i=start; i<= N; i+=step) {
			sizesSPSS.append(resultsSPSS[i].formatSizes());
			sizesAware.append(resultsAware[i].formatSizes());
			sizesSimple.append(resultsSimple[i].formatSizes());			
		}

		WorkflowLog.stringToFile(sizesSPSS.toString(), prefix + dags[0] + "b" + budget + "h" +start + "-" + N + "m" + max_scaling + "run" + runID + "-sizesSPSS.txt");
		WorkflowLog.stringToFile(sizesAware.toString(), prefix + dags[0] + "b" + budget + "h" +start + "-" + N + "m" + max_scaling + "run" + runID + "-sizesAware.txt");
		WorkflowLog.stringToFile(sizesSimple.toString(), prefix + dags[0] + "b" + budget + "h" +start + "-" + N + "m" + max_scaling + "run" + runID + "-sizesSimple.txt");			

		
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
	
	public static void runSeries(String prefix, String dagPath, String[] dags, double budget, double price,
			 int N, int step, int start, double max_scaling) {
		
		runSeries(prefix, dagPath, dags, budget, price, N, step, start, max_scaling, 0);
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
	
	public static void runSeries(String prefix, String dagPath, String dagName, double budget, double price,
			int numDAGs, int N, int step, int start, double max_scaling, int runID) {
		
		String[] dags = new String[numDAGs];
		
		for (int i=0; i< numDAGs; i++) dags[i] = dagName;
		
		runSeries(prefix, dagPath, dags, budget, price, N, step, start, max_scaling, runID);

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
	
	public static void runSeries(String prefix, String dagPath, String dagName, double budget, double price,
			int numDAGs, int N, int step, int start, double max_scaling) {
		runSeries(prefix, dagPath, dagName, budget, price, numDAGs, N, step, start, max_scaling, 0);
	}
	
	/**
	 * Repeats a series runs times, increasing runID from 0 to runs-1
	 * @param runs number of runs
	 */
	
	public static void runSeriesRepeat(String prefix, String dagPath, String dagName, double budget, double price,
			int numDAGs, int N, int step, int start, double max_scaling, int runs) {
		
		for (int i=0; i< runs; i++) {
			runSeries(prefix, dagPath, dagName, budget, price, numDAGs, N, step, start, max_scaling, i);
		}	
	}


	/**
	 * Repeats a series runs times, increasing runID from 0 to runs-1
	 * @param runs number of runs
	 */
	
	public static void runSeriesRepeat(String prefix, String dagPath, String[] dags,
			double budget, double price, int N, int step, int start,
			double max_scaling, int runs) {

		for (int i=0; i< runs; i++) {
			runSeries(prefix, dagPath, dags, budget, price, N, step, start, max_scaling, i);
		}
	}

}
