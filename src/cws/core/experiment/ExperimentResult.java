package cws.core.experiment;

import java.util.List;



/**
 * Class for storing the experiment results and storing them in a text format.
 * @author malawski
 *
 */
public class ExperimentResult {
	
	private String algorithm;
	private double cost;
	private double budget;
	private int numFreeVMs;
	private int numBusyVMs;
	private int numTotalVMs;
	private int numFinishedDAGs;
	private double deadline;
	private List<Integer> priorities;
	private List<Double> sizes;
	private String scoreBitString;
	private double actualFinishTime;
	private long planningWallTime;
	private long simulationWallTime;
	private long initWallTime;
	


	public String getAlgorithm() {
		return algorithm;
	}
	public void setAlgorithm(String algorithm) {
		this.algorithm = algorithm;
	}
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
	public long getPlanningWallTime() {
		return planningWallTime;
	}
	public void setPlanningWallTime(long planningWallTime) {
		this.planningWallTime = planningWallTime;
	}
	public long getSimulationWallTime() {
		return simulationWallTime;
	}
	public void setSimulationWallTime(long simulationWallTime) {
		this.simulationWallTime = simulationWallTime;
	}
	public long getInitWallTime() {
		return initWallTime;
	}
	public void setInitWallTime(long initWallTime) {
		this.initWallTime = initWallTime;
	}	
	public double getActualFinishTime() {
		return actualFinishTime;
	}
	public void setActualFinishTime(double actualFinishTime) {
		this.actualFinishTime = actualFinishTime;
	}
	public String getScoreBitString() {
		return scoreBitString;
	}
	public void setScoreBitString(String scoreBitString) {
		this.scoreBitString = scoreBitString;
	}
	
	/**
	 * Format the list of priorities of completed DAGs as a string
	 * @return the string containing space separated priorities
	 */
	
	public String formatPriorities() {
		
		StringBuffer prioritiesBuffer = new StringBuffer();
		
		for (int priority : getPriorities()) {
			prioritiesBuffer.append(priority + " ");
		}
		
		//remove trailing space
		if (prioritiesBuffer.length() >0) prioritiesBuffer.deleteCharAt(prioritiesBuffer.length()-1);
		prioritiesBuffer.append("\n");
		
		return prioritiesBuffer.toString();
	}
	
	/**
	 * Format the list of sizes of completed DAGs as a string
	 * @return the string containing space separated sizes
	 */
	
	public String formatSizes() {
		
		StringBuffer sizesBuffer = new StringBuffer();
		
		for (double size : getSizes()) {
			sizesBuffer.append(size + " ");
		}
		if (sizesBuffer.length()>0) sizesBuffer.deleteCharAt(sizesBuffer.length()-1);
		sizesBuffer.append("\n");
		
		return sizesBuffer.toString();
	}
	
	public String formatResult() {
		StringBuilder result = new StringBuilder();
		result.append("algorithm " + algorithm + "\n");
		result.append("budget " + budget + "\n");
		result.append("deadline " + deadline + "\n");
		result.append("cost " + cost + "\n");
		result.append("finished " + numFinishedDAGs + "\n");
		result.append("priorities " + formatPriorities());
		result.append("sizes " + formatSizes());
		result.append("actualFinishTime " + actualFinishTime + "\n");
		result.append("scoreBitString " + scoreBitString + "\n");
		result.append("initWallTime " + initWallTime + "\n");
		result.append("planningWallTime " + planningWallTime + "\n");
		result.append("simulationWallTime " + simulationWallTime + "\n");
		return result.toString();
	}

	

}
