package cws.core.experiment;


import org.junit.Test;


/**
 * Tests series of ensembles of the same workflow repeated N times.
 * @author malawski
 *
 */

public class ExperimentTest {

	String dagPath = "dags/";
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
		
		dagName = "Montage_1000.dag";

		N = 20;
		step = 1;
		start = 1;
		numDAGs = 100;
		max_scaling = 0;
		
		double[] budgets= {80.0, 160.0, 240.0, 320.0, 400.0};
		
		for (double budget : budgets) {
			Experiment.runSeries(dagPath, dagName, budget, price, numDAGs, N, step, start, max_scaling);			
		}		
		
	}

	@Test
	public void testRunExperimentCybershake0() {		
		
		dagName = "CyberShake_1000.dag";
		N = 20;
		step = 1;
		start = 1;
		numDAGs = 100;
		max_scaling = 0;

		
		double[] budgets= {80.0, 160.0, 240.0, 320.0, 400.0};
		
		for (double budget : budgets) {
			Experiment.runSeries(dagPath, dagName, budget, price, numDAGs, N, step, start, max_scaling);			
		}
		
	}
	
	@Test
	public void testRunExperimentInspiral0() {
		
		dagName = "Inspiral_1000.dag";
		N = 40;
		step = 1;
		start = 1;
		numDAGs = 40;
		max_scaling = 0;


		double[] budgets= {400.0, 800.0, 1200.0, 1600.0, 2000.0};

		for (double budget : budgets) {
			Experiment.runSeries(dagPath, dagName, budget, price, numDAGs, N, step, start, max_scaling);			
		}
		
	}
	
	
	@Test
	public void testRunExperimentEpigenomics0() {
		
		dagName = "Epigenomics_997.dag";
		N = 1500;
		step = 10;
		start = 10;
		numDAGs = 40;
		max_scaling = 0;

		
		double[] budgets= {8000.0, 16000.0, 24000.0, 32000.0, 40000.0};
		
		for (double budget : budgets) {
			Experiment.runSeries(dagPath, dagName, budget, price, numDAGs, N, step, start, max_scaling);			
		}
			
	}

	@Test
	public void testRunExperimentSipht0() {
		
		dagName = "Sipht_1000.dag";
		N = 50;
		step = 1;
		start = 1;
		numDAGs = 40;
		max_scaling = 0;

		
		double[] budgets= {200.0, 400.0, 600.0, 800.0, 1000.0};

		for (double budget : budgets) {
			Experiment.runSeries(dagPath, dagName, budget, price, numDAGs, N, step, start, max_scaling);			
		}
		
	}

	@Test
	public void testRunExperimentPsload_large0() {
		
		dagName = "psload_large.dag";
		N = 30;
		step = 1;
		start = 1;
		numDAGs = 40;
		max_scaling = 0;

		
		double[] budgets= {200.0, 400.0, 600.0, 800.0, 1000.0};

		for (double budget : budgets) {
			Experiment.runSeries(dagPath, dagName, budget, price, numDAGs, N, step, start, max_scaling);			
		}
		
	}

	@Test
	public void testRunExperimentPsload_medium0() {
		
		dagName = "psload_medium.dag";
		N = 50;
		step = 1;
		start = 1;
		numDAGs = 40;
		max_scaling = 0;

		
		double[] budgets= {20.0, 40.0, 60.0, 80.0, 100.0};

		for (double budget : budgets) {
			Experiment.runSeries(dagPath, dagName, budget, price, numDAGs, N, step, start, max_scaling);			
		}
		
	}
	
	
	@Test
	public void testRunExperimentPsmerge_small0() {
		
		dagName = "psmerge_small.dag";
		N = 150;
		step = 5;
		start = 5;
		numDAGs = 40;
		max_scaling = 0;

		
		double[] budgets= {2000.0, 4000.0, 6000.0, 8000.0, 10000.0};

		for (double budget : budgets) {
			Experiment.runSeries(dagPath, dagName, budget, price, numDAGs, N, step, start, max_scaling);			
		}
		
	}
	
	/******************************
	 * 
	 * Tests with max scaling = 2.0
	 * 
	 ******************************/
	
	
	@Test
	public void testRunExperimentMontage2() {		
		
		dagName = "Montage_1000.dag";

		N = 20;
		step = 1;
		start = 1;
		numDAGs = 100;
		max_scaling = 2.0;
		
		double[] budgets= {80.0, 160.0, 240.0, 320.0, 400.0};
		
		for (double budget : budgets) {
			Experiment.runSeries(dagPath, dagName, budget, price, numDAGs, N, step, start, max_scaling);			
		}		
		
	}

	@Test
	public void testRunExperimentCybershake2() {		
		
		dagName = "CyberShake_1000.dag";
		N = 20;
		step = 1;
		start = 1;
		numDAGs = 100;
		max_scaling = 2.0;
		
		double[] budgets= {80.0, 160.0, 240.0, 320.0, 400.0};
		
		for (double budget : budgets) {
			Experiment.runSeries(dagPath, dagName, budget, price, numDAGs, N, step, start, max_scaling);			
		}
		
	}
	
	@Test
	public void testRunExperimentInspiral2() {
		
		dagName = "Inspiral_1000.dag";
		N = 40;
		step = 1;
		start = 1;
		numDAGs = 40;
		max_scaling = 2.0;

		double[] budgets= {400.0, 800.0, 1200.0, 1600.0, 2000.0};

		for (double budget : budgets) {
			Experiment.runSeries(dagPath, dagName, budget, price, numDAGs, N, step, start, max_scaling);			
		}
		
	}
	
	
	@Test
	public void testRunExperimentEpigenomics2() {
		
		dagName = "Epigenomics_997.dag";
		N = 1500;
		step = 10;
		start = 10;
		numDAGs = 40;
		max_scaling = 2.0;
		
		double[] budgets= {8000.0, 16000.0, 24000.0, 32000.0, 40000.0};
		
		for (double budget : budgets) {
			Experiment.runSeries(dagPath, dagName, budget, price, numDAGs, N, step, start, max_scaling);			
		}
			
	}

	@Test
	public void testRunExperimentSipht2() {
		
		dagName = "Sipht_1000.dag";
		N = 50;
		step = 1;
		start = 1;
		numDAGs = 40;
		max_scaling = 2.0;
		
		double[] budgets= {200.0, 400.0, 600.0, 800.0, 1000.0};

		for (double budget : budgets) {
			Experiment.runSeries(dagPath, dagName, budget, price, numDAGs, N, step, start, max_scaling);			
		}
		
	}
	
	@Test
	public void testRunExperimentPsload_large2() {
		
		dagName = "psload_large.dag";
		N = 30;
		step = 1;
		start = 1;
		numDAGs = 40;
		max_scaling = 2.0;

		
		double[] budgets= {200.0, 400.0, 600.0, 800.0, 1000.0};

		for (double budget : budgets) {
			Experiment.runSeries(dagPath, dagName, budget, price, numDAGs, N, step, start, max_scaling);			
		}
		
	}

	@Test
	public void testRunExperimentPsload_medium2() {
		
		dagName = "psload_medium.dag";
		N = 50;
		step = 1;
		start = 1;
		numDAGs = 40;
		max_scaling = 2.0;

		
		double[] budgets= {20.0, 40.0, 60.0, 80.0, 100.0};

		for (double budget : budgets) {
			Experiment.runSeries(dagPath, dagName, budget, price, numDAGs, N, step, start, max_scaling);			
		}
		
	}
	
	
	@Test
	public void testRunExperimentPsmerge_small2() {
		
		dagName = "psmerge_small.dag";
		N = 150;
		step = 5;
		start = 5;
		numDAGs = 40;
		max_scaling = 2.0;

		
		double[] budgets= {2000.0, 4000.0, 6000.0, 8000.0, 10000.0};

		for (double budget : budgets) {
			Experiment.runSeries(dagPath, dagName, budget, price, numDAGs, N, step, start, max_scaling);			
		}
		
	}
	
	@Test
	public void testRunSeriesRepeatPsmerge_small2_10() {
		
		dagName = "psmerge_small.dag";
		N = 150;
		step = 5;
		start = 5;
		numDAGs = 40;
		max_scaling = 2.0;

		
		double[] budgets= {2000.0, 4000.0, 6000.0, 8000.0, 10000.0};

		for (double budget : budgets) {
			Experiment.runSeriesRepeat(dagPath, dagName, budget, price, numDAGs, N, step, start, max_scaling, 10);			
		}
		
	}



	
}
