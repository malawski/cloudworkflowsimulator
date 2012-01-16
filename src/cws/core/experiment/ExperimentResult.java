package cws.core.experiment;

import java.util.List;

public class ExperimentResult {
	
	private double cost;
	private double budget;
	private int numFreeVMs;
	private int numBusyVMs;
	private int numTotalVMs;
	private int numFinishedDAGs;
	private double deadline;
	private List<Integer> priorities;
	private List<Double> sizes;
	


	public double getCost() {
		return cost;
	}
	public void setCost(double cost) {
		this.cost = cost;
	}
	public double getBudget() {
		return budget;
	}
	public void setBudget(double budget) {
		this.budget = budget;
	}
	public int getNumFreeVMs() {
		return numFreeVMs;
	}
	public void setNumFreeVMs(int numFreeVMs) {
		this.numFreeVMs = numFreeVMs;
	}
	public int getNumBusyVMs() {
		return numBusyVMs;
	}
	public void setNumBusyVMs(int numBusyVMs) {
		this.numBusyVMs = numBusyVMs;
	}
	public int getNumTotalVMs() {
		return numTotalVMs;
	}
	public void setNumTotalVMs(int numTotalVMs) {
		this.numTotalVMs = numTotalVMs;
	}
	public int getNumFinishedDAGs() {
		return numFinishedDAGs;
	}
	public void setNumFinishedDAGs(int numFinishedDAGs) {
		this.numFinishedDAGs = numFinishedDAGs;
	}
	public double getDeadline() {
		return deadline;
	}
	public void setDeadline(double deadline) {
		this.deadline = deadline;
	}
	public List<Integer> getPriorities() {
		return priorities;
	}
	public void setPriorities(List<Integer> priorities) {
		this.priorities = priorities;
	}
	public List<Double> getSizes() {
		return sizes;
	}
	public void setSizes(List<Double> sizes) {
		this.sizes = sizes;
	}
	
	/**
	 * Format the list of priorities of completed DAGs as a string
	 * @return the string containing: deadline and space separated priorities
	 */
	
	public String formatPriorities() {
		
		StringBuffer prioritiesBuffer = new StringBuffer();

		prioritiesBuffer.append(getDeadline());
		
		for (int priority : getPriorities()) {
			prioritiesBuffer.append(" " + priority);
		}
		
		prioritiesBuffer.append("\n");
		
		return prioritiesBuffer.toString();
	}
	
	/**
	 * Format the list of sizes of completed DAGs as a string
	 * @return the string containing: deadline and space separated sizes
	 */
	
	public String formatSizes() {
		
		StringBuffer sizesBuffer = new StringBuffer();

		sizesBuffer.append(getDeadline());
		
		for (double size : getSizes()) {
			sizesBuffer.append(" " + size);
		}
		
		sizesBuffer.append("\n");
		
		return sizesBuffer.toString();
	}
	

}
