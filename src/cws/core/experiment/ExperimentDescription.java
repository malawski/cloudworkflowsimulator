package cws.core.experiment;

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
	// provisioner implementation to use
	private Provisioner provisioner;
	// scheduler implementation to use
	private Scheduler scheduler;
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

	
	/**
	 * This description creates workflow ensemble with the same DAG (dagName) repeating numDAGs times.
	 * 
	 * @param provisioner provisioner implementation
	 * @param scheduler scheduler implementation
	 * @param dagPath path to DAG files
	 * @param dagName name of DAG to repeat in the ensemble
	 * @param deadline deadline in seconds
	 * @param budget budget in $
	 * @param price VM hour price in $
	 * @param numDAGs number of times a DAG is repeated in the ensemble
	 * @param max_scaling maximum autoscaling factor for provisioner
	 */
	
	public ExperimentDescription(Provisioner provisioner, Scheduler scheduler,
			String dagPath, String dagName, double deadline, double budget, double price,
			int numDAGs, double max_scaling) { 
		this(provisioner, scheduler, dagPath, (String[]) null, deadline, budget, price,  max_scaling);
		String[] dags = new String[numDAGs];
		for (int i=0; i< numDAGs; i++) dags[i] = dagName;
		this.setDags(dags);
	}
	
	/**
	 * This description creates ensemble of workflows given in the dags array. 
	 * 
	 * @param provisioner provisioner implementation
	 * @param scheduler scheduler implementation
	 * @param dagPath path to DAG files
	 * @param dags array of DAG file names
	 * @param deadline deadline in seconds
	 * @param budget budget in $
	 * @param price VM hour price in $
	 * @param numDAGs number of times a DAG is repeated in the ensemble
	 * @param max_scaling maximum autoscaling factor for provisioner
	 */
	
	public ExperimentDescription(Provisioner provisioner, Scheduler scheduler,
			String dagPath, String[] dags, double deadline, double budget, double price,
			double max_scaling) {
		this.provisioner = provisioner;
		this.scheduler = scheduler;
		this.dags = dags;
		this.deadline = deadline;
		this.budget = budget;
		this.price = price;
		this.max_scaling = max_scaling;
		this.dagPath = dagPath;
	}
	
	public Provisioner getProvisioner() {
		return provisioner;
	}

	public void setProvisioner(Provisioner provisioner) {
		this.provisioner = provisioner;
	}

	public Scheduler getScheduler() {
		return scheduler;
	}

	public void setScheduler(Scheduler scheduler) {
		this.scheduler = scheduler;
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

}