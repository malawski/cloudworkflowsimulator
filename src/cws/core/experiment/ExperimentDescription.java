package cws.core.experiment;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;

import cws.core.Provisioner;
import cws.core.Scheduler;

/**
 * 
 * @author malawski
 * 
 * Class representing a single experiment of submitting a given 
 * workflow ensemble with specified budget and deadline constraints.
 *
 */

public class ExperimentDescription {

	// name of algorithm to use
	private String algorithmName;
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
	private double max_scaling;
	// alpha parameter for SPSS
	private double alpha;

	
	
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
	 */
	
	public ExperimentDescription(String algorithmName, String dagPath, String[] dags, double deadline, double budget, double price,
			double max_scaling, double alpha) {
		this.algorithmName = algorithmName;
		this.dags = dags;
		this.deadline = deadline;
		this.budget = budget;
		this.price = price;
		this.max_scaling = max_scaling;
		this.dagPath = dagPath;
		this.alpha = alpha;
	}
	
	public ExperimentDescription(String propertyFileName) {
		readProperties(propertyFileName);
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
		this.max_scaling = max_scaling;
	}

	public double getMax_scaling() {
		return max_scaling;
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

	public double getAlpha() {
		return alpha;
	}

	public void setAlpha(double alpha) {
		this.alpha = alpha;
	}

	public void storeProperties(String fileName) {
		Properties p = new Properties();
		p.setProperty("algorithmName", algorithmName);
		p.setProperty("dagPath", dagPath);
		p.setProperty("dags", dagsToString());
		p.setProperty("deadline", "" + deadline);
		p.setProperty("budget", "" + budget);
		p.setProperty("price", "" + price);
		p.setProperty("max_scaling", "" + max_scaling);
		p.setProperty("alpha", "" + alpha);
		FileOutputStream out;
		try {
			out = new FileOutputStream(fileName);
			p.store(out, "");
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	private void readProperties(String fileName) {
		Properties p = new Properties();
		try {
			FileInputStream in = new FileInputStream(fileName);
			p.load(in);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		algorithmName = p.getProperty("algorithmName");
		dagPath = p.getProperty("dagPath");
		dags = dagsFromString(p.getProperty("dags"));
		deadline = Double.parseDouble(p.getProperty("deadline"));
		budget = Double.parseDouble(p.getProperty("budget"));
		price = Double.parseDouble(p.getProperty("price"));
		max_scaling = Double.parseDouble(p.getProperty("max_scaling"));
		alpha = Double.parseDouble(p.getProperty("alpha"));
		
	}
	
	private String[] dagsFromString(String property) {
		return property.split(" ");
	}

	private String dagsToString() {
		StringBuilder s = new StringBuilder();
		for (String dag : dags) s.append(dag + " ");
		return s.toString();
	}
	
}