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

public class GenerateExperimentDistributionsFractions {
    
	
	/******************************
	 * 
	 * Tests with max scaling = 0.0
	 * 
	 ******************************/
	
	public static void main(String [] args) {		

		String dagPath;
		String dagName;
		double price = 1.0;
		double maxScaling = 0.0;
		String group = "fractions-nodelays-50";
		double alpha = 0.7;
		String runDirectory = ""; 
		int runID = 0;
		double taskDilatation = 1.0;
		double runtimeVariation = 0.0;
		double delay = 0.0;
		String[] distributions = {"uniform_unsorted", "uniform_sorted", "pareto_unsorted", "pareto_sorted", "constant"} ;
		int ensemble_size = 50;
		
		
		String algorithms[] = {"SPSS", "DPDS", "WADPDS"};
//		String algorithms[] = {"SPSS", "DPDS", "WADPDS", "MaxMin", "Wide", "Backtrack"};
		
		int numRunIDs = 10;
		
		for (String distribution: distributions) {
		
			for (runID = 0; runID < numRunIDs; runID++) {

				runDirectory = String.format("run-%03d-%s", runID, group);

				dagPath = "../projects/pegasus/Montage/";
				dagName = "MONTAGE";
				Experiment.generateSeries(runDirectory, group, dagPath, dagName, ensemble_size, distribution , algorithms, price, maxScaling, alpha,  taskDilatation, runtimeVariation, delay, runID);	

				dagPath = "../projects/pegasus/CyberShake/";
				dagName = "CYBERSHAKE";
				Experiment.generateSeries(runDirectory, group, dagPath, dagName, ensemble_size, distribution , algorithms, price, maxScaling, alpha,  taskDilatation, runtimeVariation, delay, runID);	

				dagPath = "../projects/pegasus/LIGO/";
				dagName = "LIGO";
				Experiment.generateSeries(runDirectory, group, dagPath, dagName, ensemble_size, distribution , algorithms, price, maxScaling, alpha,  taskDilatation, runtimeVariation, delay, runID);	

				dagPath = "../projects/pegasus/Genome/";
				dagName = "GENOME";
				Experiment.generateSeries(runDirectory, group, dagPath, dagName, ensemble_size, distribution , algorithms, price, maxScaling, alpha,  taskDilatation, runtimeVariation, delay, runID);	

				dagPath = "../projects/pegasus/SIPHT/";
				dagName = "SIPHT";
				Experiment.generateSeries(runDirectory, group, dagPath, dagName, ensemble_size, distribution , algorithms, price, maxScaling, alpha,  taskDilatation, runtimeVariation, delay, runID);	


			}	
		}

	}
		
}


	



	

