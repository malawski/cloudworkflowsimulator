package cws.core.experiment;


import java.util.Random;

import org.junit.Test;


/**
 * Generates series of ensembles consisting of workflows form workflow generator  
 * https://confluence.pegasus.isi.edu/display/pegasus/WorkflowGenerator
 * 
 * From each workflow type DAGs of Pareto - distributed sizes are selected. 
 * 
 * @author malawski
 *
 */

public class GenerateExperimentParetoFractions {
    
	
	/******************************
	 * 
	 * Tests with max scaling = 0.0
	 * 
	 ******************************/
	
	public static void main(String [] args) {		

		String dagPath;
		String dagName;
		double price = 1.0;
		double maxScaling = 2.0;
		String group = "fractions-nodelays";
		double alpha = 0.7;
		String[] dags;
		double[] budgets;
		String runDirectory = "run-01-pareto"; 
		int runID = 0;
		double taskDilatation = 1.0;
		
		double maxHours;
		double stepHours;
		double startHours;
		
		int numRunIDs = 1;
		
		for (runID = 0; runID < numRunIDs; runID++) {
			
			runDirectory = String.format("run-%03d-%s", runID, group);


			dagPath = "../projects/pegasus/Montage/";
			dagName = "MONTAGE";

			dags = DAGListGenerator.generateDAGListPareto(new Random(runID), dagName, 50);
	        int ndeadlines = 10;

			maxHours = 4858/3600.0;
			startHours = 559/3600.0;
			stepHours = (maxHours-startHours) / (ndeadlines -1);
			maxScaling = 0;

	        int nbudgets = 10;
			double minBudget = 1.0;
	        double maxBudget = 27.0;
	        double budgetStep = (maxBudget - minBudget) / (nbudgets - 1);
			
			//		budgets= new double[] {10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0, 100.0};
			budgets = new double[nbudgets];
			int i = 0;
			for (double budget = minBudget; budget <= maxBudget; budget += budgetStep, i++)
			budgets[i] = budget;

			for (double budget : budgets) {
				Experiment.generateSeries(runDirectory, group, dagPath, dags, budget, price, maxHours, stepHours, startHours, maxScaling, alpha,  taskDilatation, runID);			
			}		




		}
		
	}


	



	
}
