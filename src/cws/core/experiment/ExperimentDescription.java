package cws.core.experiment;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.commons.io.IOUtils;

/**
 * 
 * @author malawski
 * 
 * Class representing a single experiment of submitting a given 
 * workflow ensemble with specified budget and deadline constraints.
 *
 */

public class ExperimentDescription {

	// name of group this experiment belongs to
	private String group;
	// name of algorithm to use
	private String algorithmName;
	// directory of input and output files for this experiment
	private String runDirectory;
	// path to DAG files
	private String dagPath;
	// array of dag file names
	private String[] dags;
	// deadline in hours
	private double deadline;
	// budget in $
	private double budget;
	// VM hour price in $
	private double price;
	// max autoscaling factor for provisioner
	private double maxScaling;
	// alpha parameter for SPSS
	private double alpha;
	// runID used to distinguish e.g. different random seeds
	private int runID;
	// task runtimes from the dag are multiplied by this factor; this parameter is useful to control the task granularity
	private double taskDilatation;
	// defines the maximum relative difference between estimated and actual task runtime, e.g. 0.50 means that task can run 50% longer than a given estimate
	private double runtimeVariation;
	// provisioning delay in seconds
	private double delay;
	// distribution of ensembles
	private String distribution;

	
	
	/**
	 * This description creates ensemble of workflows given in the dags array. 
	 * 
	 * @param algorithmName name of the algorithm class from cws.core.algorithms
	 * @param dagPath path to DAG files
	 * @param dags array of DAG file names
	 * @param deadline deadline in seconds
	 * @param budget budget in $
	 * @param price VM hour price in $
	 * @param numDAGs number of times a DAG is repeated in the ensemble
	 * @param max_scaling maximum autoscaling factor for provisioner
	 * @param alpha alpha parameter for SPSS
	 * @param runID runID used to distinguish e.g. different random seeds
	 * @param taskDilatation task runtimes from the dag are multiplied by this factor; this parameter is useful to control the task granularity
	 */
	
	public ExperimentDescription(String group, String algorithmName, String runDirectory, String dagPath, String[] dags, double deadline, double budget, double price,
			double maxScaling, double alpha, double taskDilatation, double runtimeVariation, double delay, String distribution, int runID) {
		this.group = group;
		this.algorithmName = algorithmName;
		this.runDirectory = runDirectory;
		this.dags = dags;
		this.deadline = deadline;
		this.budget = budget;
		this.price = price;
		this.maxScaling = maxScaling;
		this.dagPath = dagPath;
		this.alpha = alpha;
		this.runID = runID;
		this.taskDilatation = taskDilatation;
		this.runtimeVariation = runtimeVariation;
		this.delay = delay;
		this.distribution = distribution;
	}
	
	public ExperimentDescription(String propertyFileName) {
		readProperties(propertyFileName);
	}

	
	
	public String getGroup() {
		return group;
	}

	public void setGroup(String group) {
		this.group = group;
	}

	public String[] getDags() {
		return dags;
	}

	public void setDags(String[] dags) {
		this.dags = dags;
	}

	public double getDeadline() {
		return deadline;
	}

	public void setDeadline(double deadline) {
		this.deadline = deadline;
	}

	public double getBudget() {
		return budget;
	}

	public void setBudget(double budget) {
		this.budget = budget;
	}

	public double getPrice() {
		return price;
	}

	public void setPrice(double price) {
		this.price = price;
	}

	public void setMax_scaling(double max_scaling) {
		this.maxScaling = max_scaling;
	}

	public double getMax_scaling() {
		return maxScaling;
	}

	public String getDagPath() {
		return dagPath;
	}

	public void setDagPath(String dagPath) {
		this.dagPath = dagPath;
	}

	public String getAlgorithmName() {
		return algorithmName;
	}

	public void setAlgorithmName(String algorithmName) {
		this.algorithmName = algorithmName;
		
	}

	public String getRunDirectory() {
		return runDirectory;
	}

	public void setRunDirectory(String runDirectory) {
		this.runDirectory = runDirectory;
	}
	
	public double getAlpha() {
		return alpha;
	}

	public void setAlpha(double alpha) {
		this.alpha = alpha;
	}

	public int getRunID() {
		return runID;
	}

	public void setRunID(int runID) {
		this.runID = runID;
	}

	public double getMaxScaling() {
		return maxScaling;
	}

	public void setMaxScaling(double maxScaling) {
		this.maxScaling = maxScaling;
	}

	public double getTaskDilatation() {
		return taskDilatation;
	}

	public void setTaskDilatation(double taskDilatation) {
		this.taskDilatation = taskDilatation;
	}
	
	public double getRuntimeVariation() {
		return runtimeVariation;
	}

	public void setRuntimeVariation(double runtimeVariation) {
		this.runtimeVariation = runtimeVariation;
	}

	public double getDelay() {
		return delay;
	}

	public void setDelay(double delay) {
		this.delay = delay;
	}
	
	public String getDistribution() {
		return distribution;
	}

	public void setDistribution(String distribution) {
		this.distribution = distribution;
	}

	public void storeProperties(String fileName) {
		Properties p = new Properties();
		p.setProperty("group", group);
		p.setProperty("algorithmName", algorithmName);
		p.setProperty("runDirectory", runDirectory);
		p.setProperty("dagPath", dagPath);
		p.setProperty("dags", dagsToString());
		p.setProperty("deadline", "" + deadline);
		p.setProperty("budget", "" + budget);
		p.setProperty("price", "" + price);
		p.setProperty("maxScaling", "" + maxScaling);
		p.setProperty("alpha", "" + alpha);
		p.setProperty("runID", "" + runID);
		p.setProperty("fileName", getFileName());
		p.setProperty("taskDilatation", "" + taskDilatation);
		p.setProperty("runtimeVariation", "" + runtimeVariation);
		p.setProperty("delay", "" + delay);
		p.setProperty("distribution", getDistribution());
		FileOutputStream out = null;
		try {
			out = new FileOutputStream(fileName);
			p.store(out, "");
		} catch (FileNotFoundException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		} finally {
			IOUtils.closeQuietly(out);
		}
	}

	private void readProperties(String fileName) {
		Properties p = new Properties();
		FileInputStream in = null;
		try {
			in = new FileInputStream(fileName);
			p.load(in);
		} catch (IOException e) {
			throw new RuntimeException(e);
		} finally {
			IOUtils.closeQuietly(in);
		}
		group = p.getProperty("group");
		algorithmName = p.getProperty("algorithmName");
		runDirectory = p.getProperty("runDirectory");
		dagPath = p.getProperty("dagPath");
		dags = dagsFromString(p.getProperty("dags"));
		deadline = Double.parseDouble(p.getProperty("deadline"));
		budget = Double.parseDouble(p.getProperty("budget"));
		price = Double.parseDouble(p.getProperty("price"));
		maxScaling = Double.parseDouble(p.getProperty("maxScaling"));
		alpha = Double.parseDouble(p.getProperty("alpha"));
		runID = Integer.parseInt(p.getProperty("runID"));
		taskDilatation = Double.parseDouble(p.getProperty("taskDilatation"));
		runtimeVariation = Double.parseDouble(p.getProperty("runtimeVariation"));
		delay = Double.parseDouble(p.getProperty("delay"));
		distribution = p.getProperty("distribution");
		
	}
	
	private String[] dagsFromString(String property) {
		return property.split(" ");
	}

	private String dagsToString() {
		StringBuilder s = new StringBuilder();
		for (String dag : dags) s.append(dag + " ");
		return s.toString();
	}
	
	
	/*
	 * Creates a unique file name based on the values of the properties
	 */
	public String getFileName() {
		String fileName = 
			group + "-" +
			getAlgorithmName() + "-" + 
			getDistribution() + "-" + 
			getDags()[0]+
			"x" + getDags().length + 
			"d" + getDeadline() + 
			"b" + getBudget() + 
			"m" + getMax_scaling() +
			"a" + getAlpha() + 
			"t" + getTaskDilatation() +
			"v" + getRuntimeVariation() +
			"l" + getDelay() +
			"r" + runID;
		
		return fileName;
		
	}
	
}