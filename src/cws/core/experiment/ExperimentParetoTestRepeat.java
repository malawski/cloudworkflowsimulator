package cws.core.experiment;


import java.util.Random;

import org.junit.Test;


/**
 * Tests series of ensembles consisting of workflows form workflow generator  
 * https://confluence.pegasus.isi.edu/display/pegasus/WorkflowGenerator
 * 
 * From each workflow type DAGs of sizes 1000 and 900 are selected. 
 * For smaller workflows (Montage, Cybershake) also smaller sizes (down to 500) are added to ensembles.
 * 
 * @author malawski
 *
 */

public class ExperimentParetoTestRepeat {
    
	String dagPath;
	String dagName;
	double budget;
	double price = 1.0;
	int numDAGs = 40;
	double max_scaling = 2.0;

	
	int N;
	int step;
	int start;
	
	/******************************
	 * 
	 * Tests with max scaling = 0.0
	 * 
	 ******************************/
	
	@Test
	public void testRunExperimentMontage0() {		

		dagPath = "../projects/pegasus/Montage/";
		dagName = "MONTAGE";
		
		String[] dags = DAGListGenerator.generateDAGListPareto(new Random(0), dagName, 100);
		N = 20;
		step = 1;
		start = 1;
		max_scaling = 0;
		
		double[] budgets= {40.0, 80.0, 120.0, 160.0, 200.0};
		
		for (double budget : budgets) {
			Experiment.runSeriesRepeat(dagPath, dags, budget, price, N, step, start, max_scaling, 1);			
		}		
		
	}

	@Test
	public void testRunExperimentCybershake0() {		

		dagPath = "../projects/pegasus/CyberShake/";
		dagName = "CYBERSHAKE";
		
		String[] dags = DAGListGenerator.generateDAGListPareto(new Random(0), dagName, 100);
		
		N = 20;
		step = 1;
		start = 1;
		max_scaling = 0;

		
		double[] budgets= {40.0, 60.0, 80.0, 100.0, 120.0};
		
		for (double budget : budgets) {
			Experiment.runSeriesRepeat(dagPath, dags, budget, price, N, step, start, max_scaling, 1);			
		}
		
	}

	
	@Test
	public void testRunExperimentCybershakePareto() {		

		dagPath = "../projects/pegasus/CyberShake/";
		dagName = "CYBERSHAKE";
		
		String[] dags = DAGListGenerator.generateDAGListPareto(new Random(0), dagName, 100);
		
		N = 14;
		step = 1;
		start = 14;
		max_scaling = 0;

		
		double[] budgets= {100.0};
		
		for (double budget : budgets) {
			Experiment.runSeriesRepeat(dagPath, dags, budget, price, N, step, start, max_scaling, 1);			
		}
		
	}
	
	@Test
	public void testRunExperimentInspiral0() {
		
		dagPath = "../projects/pegasus/LIGO/";
		dagName = "LIGO";
		
		String[] dags = DAGListGenerator.generateDAGListPareto(new Random(0), dagName, 40);
		
		N = 40;
		step = 1;
		start = 1;
		max_scaling = 0;


		double[] budgets= {400.0, 800.0, 1200.0, 1600.0, 2000.0};

		for (double budget : budgets) {
			Experiment.runSeriesRepeat(dagPath, dags, budget, price, N, step, start, max_scaling, 1);			
		}
		
	}
	
	
	@Test
	public void testRunExperimentEpigenomics0() {
		
		dagPath = "../projects/pegasus/Genome/";
		dagName = "GENOME";
		
		String[] dags = DAGListGenerator.generateDAGListPareto(new Random(0), dagName, 40);
		
		N = 1500;
		step = 10;
		start = 10;
		max_scaling = 0;

		
		double[] budgets= {4000.0, 8000.0, 12000.0, 16000.0, 20000.0};
		
		for (double budget : budgets) {
			Experiment.runSeriesRepeat(dagPath, dags, budget, price, N, step, start, max_scaling, 1);			
		}
			
	}

	@Test
	public void testRunExperimentSipht0() {
		
		dagPath = "../projects/pegasus/SIPHT/";
		dagName = "SIPHT";
		
		String[] dags = DAGListGenerator.generateDAGListPareto(new Random(0), dagName, 40);

		N = 50;
		step = 1;
		start = 1;
		max_scaling = 0;

		
		double[] budgets= {200.0, 400.0, 600.0, 800.0, 1000.0};

		for (double budget : budgets) {
			Experiment.runSeriesRepeat(dagPath, dags, budget, price, N, step, start, max_scaling, 1);			
		}
		
	}


	



	
}
