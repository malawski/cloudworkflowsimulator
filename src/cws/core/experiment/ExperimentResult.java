package cws.core.experiment;

public class ExperimentResult {
	
	private double cost;
	private double budget;
	private int numFreeVMs;
	private int numBusyVMs;
	private int numTotalVMs;
	private int numFinishedDAGs;
	private double deadline;

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

}
