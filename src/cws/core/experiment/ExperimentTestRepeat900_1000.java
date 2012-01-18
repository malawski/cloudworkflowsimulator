package cws.core.experiment;


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

public class ExperimentTestRepeat900_1000 {
    
	String dagPath;
	String dagName;
	double budget;
	double price = 1.0;
	int numDAGs = 40;
	double max_scaling = 2.0;
	String prefix = "testRepeat900_1000";
	
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
		
		String[] dags = DAGListGenerator.generateDAGList(dagName, new int[] {1000, 900, 800, 700, 600, 500}, 20);

		N = 20;
		step = 1;
		start = 1;
		max_scaling = 0;
		
		double[] budgets= {40.0, 80.0, 120.0, 160.0, 200.0};
		
		for (double budget : budgets) {
			Experiment.runSeriesRepeat(prefix, dagPath, dags, budget, price, N, step, start, max_scaling, 20);			
		}		
		
	}

	@Test
	public void testRunExperimentCybershake0() {		

		dagPath = "../projects/pegasus/CyberShake/";
		dagName = "CYBERSHAKE";
		
		String[] dags = DAGListGenerator.generateDAGList(dagName, new int[] {1000, 900}, 20);
		
		N = 20;
		step = 1;
		start = 1;
		max_scaling = 0;

		
		double[] budgets= {40.0, 80.0, 120.0, 160.0, 200.0};
		
		for (double budget : budgets) {
			Experiment.runSeriesRepeat(prefix, dagPath, dags, budget, price, N, step, start, max_scaling, 20);			
		}
		
	}
	
	@Test
	public void testRunExperimentInspiral0() {
		
		dagPath = "../projects/pegasus/LIGO/";
		dagName = "LIGO";
		
		String[] dags = DAGListGenerator.generateDAGList(dagName, new int[] {1000, 900}, 20);
		
		N = 40;
		step = 1;
		start = 1;
		max_scaling = 0;


		double[] budgets= {400.0, 800.0, 1200.0, 1600.0, 2000.0};

		for (double budget : budgets) {
			Experiment.runSeriesRepeat(prefix, dagPath, dags, budget, price, N, step, start, max_scaling, 20);			
		}
		
	}
	
	
	@Test
	public void testRunExperimentEpigenomics0() {
		
		dagPath = "../projects/pegasus/Genome/";
		dagName = "GENOME";
		
		String[] dags = DAGListGenerator.generateDAGList(dagName, new int[] {1000, 900}, 20);
		
		N = 1500;
		step = 10;
		start = 10;
		max_scaling = 0;

		
		double[] budgets= {4000.0, 8000.0, 12000.0, 16000.0, 20000.0};
		
		for (double budget : budgets) {
			Experiment.runSeriesRepeat(prefix, dagPath, dags, budget, price, N, step, start, max_scaling, 20);			
		}
			
	}

	@Test
	public void testRunExperimentSipht0() {
		
		dagPath = "../projects/pegasus/SIPHT/";
		dagName = "SIPHT";
		
		String[] dags = DAGListGenerator.generateDAGList(dagName, new int[] {1000, 900}, 20);

		N = 50;
		step = 1;
		start = 1;
		max_scaling = 0;

		
		double[] budgets= {200.0, 400.0, 600.0, 800.0, 1000.0};

		for (double budget : budgets) {
			Experiment.runSeriesRepeat(prefix, dagPath, dags, budget, price, N, step, start, max_scaling, 20);			
		}
		
	}


	
	/******************************
	 * 
	 * Tests with max scaling = 2.0
	 * 
	 ******************************/
	
	
	@Test
	public void testRunExperimentMontage2() {		
		
		dagPath = "../projects/pegasus/Montage/";
		dagName = "MONTAGE";
		
		String[] dags = DAGListGenerator.generateDAGList(dagName, new int[] {1000, 900, 800, 700, 600, 500}, 20);

		N = 20;
		step = 1;
		start = 1;
		max_scaling = 2.0;
		
		double[] budgets= {40.0, 80.0, 120.0, 160.0, 200.0};
		
		for (double budget : budgets) {
			Experiment.runSeriesRepeat(prefix, dagPath, dags, budget, price, N, step, start, max_scaling, 20);			
		}		
		
	}

	@Test
	public void testRunExperimentCybershake2() {		
		
		dagPath = "../projects/pegasus/CyberShake/";
		dagName = "CYBERSHAKE";
		
		String[] dags = DAGListGenerator.generateDAGList(dagName, new int[] {1000, 900}, 20);
		
		N = 20;
		step = 1;
		start = 1;
		max_scaling = 2.0;

		
		double[] budgets= {40.0, 80.0, 120.0, 160.0, 200.0};
		
		for (double budget : budgets) {
			Experiment.runSeriesRepeat(prefix, dagPath, dags, budget, price, N, step, start, max_scaling, 20);			
		}
		
	}
	
	@Test
	public void testRunExperimentInspiral2() {
		
		dagPath = "../projects/pegasus/LIGO/";
		dagName = "LIGO";
		
		String[] dags = DAGListGenerator.generateDAGList(dagName, new int[] {1000, 900}, 20);
		
		N = 40;
		step = 1;
		start = 1;
		max_scaling = 2.0;


		double[] budgets= {400.0, 800.0, 1200.0, 1600.0, 2000.0};

		for (double budget : budgets) {
			Experiment.runSeriesRepeat(prefix, dagPath, dags, budget, price, N, step, start, max_scaling, 20);			
		}
		
	}
	
	
	@Test
	public void testRunExperimentEpigenomics2() {
		
		dagPath = "../projects/pegasus/Genome/";
		dagName = "GENOME";
		
		String[] dags = DAGListGenerator.generateDAGList(dagName, new int[] {1000, 900}, 20);
		
		N = 1500;
		step = 10;
		start = 10;
		max_scaling = 2.0;

		
		double[] budgets= {4000.0, 8000.0, 12000.0, 16000.0, 20000.0};
		
		for (double budget : budgets) {
			Experiment.runSeriesRepeat(prefix, dagPath, dags, budget, price, N, step, start, max_scaling, 20);			
		}
			
	}

	@Test
	public void testRunExperimentSipht2() {
		
		dagPath = "../projects/pegasus/SIPHT/";
		dagName = "SIPHT";
		
		String[] dags = DAGListGenerator.generateDAGList(dagName, new int[] {1000, 900}, 20);

		N = 50;
		step = 1;
		start = 1;
		max_scaling = 2.0;

		
		double[] budgets= {200.0, 400.0, 600.0, 800.0, 1000.0};

		for (double budget : budgets) {
			Experiment.runSeriesRepeat(prefix, dagPath, dags, budget, price,  N, step, start, max_scaling, 20);			
		}
		
	}
	



	
}
