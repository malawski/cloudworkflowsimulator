package cws.core.experiment;
/**
 * Tests series of ensembles consisting of workflows form workflow generator  
 * https://confluence.pegasus.isi.edu/display/pegasus/WorkflowGenerator
 * 
 * From each workflow type DAGs of sizes 1000 selected. 
 * 
 * @author malawski
 *
 */

public class GenerateExperimentConstant {
    
	public static void main(String [] args) {
	
	
	String dagPath;
	String dagName;
	double price = 1.0;
	double maxScaling = 0.0;
	String group = "constant";
	double alpha = 0.7;
	String runDirectory = "run-02-constant"; 
	int maxHours;
	int step;
	int start;
	int runID = 0;
	double taskDilatation = 1.0;
	double runtimeVariation = 0.0;
	double delay = 0.0;
	String distribution = "constant";

	


		dagPath = "../projects/pegasus/Montage/";
		dagName = "MONTAGE";
		
		String[] dags = DAGListGenerator.generateDAGListConstant(dagName, 1000, 100);
		maxHours = 20;
		step = 1;
		start = 1;
		maxScaling = 0;
		
//		double[] budgets= {10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0, 100.0};
		double[] budgets= {40.0, 80.0, 120.0, 160.0, 200.0};
		
		for (double budget : budgets) {
			Experiment.generateSeries(runDirectory, group, dagPath, dags, budget, price, maxHours, step, start, maxScaling, alpha,  taskDilatation, runtimeVariation, delay, distribution, runID);			
		}		
		

		dagPath = "../projects/pegasus/CyberShake/";
		dagName = "CYBERSHAKE";
		
		dags = DAGListGenerator.generateDAGListConstant(dagName, 1000, 100);
		
		maxHours = 20;
		step = 1;
		start = 1;
		maxScaling = 0;

		
//		budgets = new double[] {10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 80.0, 100.0, 120.0, 140.0};
		budgets = new double[] {50.0, 150.0, 250.0, 350.0, 450.0};
		
		for (double budget : budgets) {
			Experiment.generateSeries(runDirectory, group, dagPath, dags, budget, price, maxHours, step, start, maxScaling, alpha,  taskDilatation, runtimeVariation, delay, distribution, runID);			
		}
		

		
		dagPath = "../projects/pegasus/LIGO/";
		dagName = "LIGO";
		
		dags = DAGListGenerator.generateDAGListConstant(dagName, 1000, 100);
		
		maxHours = 40;
		step = 1;
		start = 1;
		maxScaling = 0;


//		budgets = new double[] {200.0, 400.0, 600.0, 800.0, 1000.0, 1200.0, 1400.0, 1600.0, 1800.0, 2000.0};
		budgets = new double[] {500.0, 1000.0, 1500.0, 2000.0, 2500.0};

		for (double budget : budgets) {
			Experiment.generateSeries(runDirectory, group, dagPath, dags, budget, price, maxHours, step, start, maxScaling, alpha,  taskDilatation, runtimeVariation, delay, distribution, runID);			
		}
		
		
		dagPath = "../projects/pegasus/Genome/";
		dagName = "GENOME";
		
		dags = DAGListGenerator.generateDAGListConstant(dagName, 1000, 100);
		
		maxHours = 1500;
		step = 100;
		start = 100;
		maxScaling = 0;

		
//		budgets = new double[] {2000.0, 4000.0, 6000.0, 8000.0, 10000.0, 12000.0, 14000.0, 16000.0, 18000.0, 20000.0};
		budgets = new double[] {5000.0, 10000.0, 15000.0, 20000.0, 25000.0};

		
		for (double budget : budgets) {
			Experiment.generateSeries(runDirectory, group, dagPath, dags, budget, price, maxHours, step, start, maxScaling, alpha,  taskDilatation, runtimeVariation, delay, distribution, runID);			
		}
			

		
		dagPath = "../projects/pegasus/SIPHT/";
		dagName = "SIPHT";
		
		dags = DAGListGenerator.generateDAGListConstant(dagName, 1000, 100);

		maxHours = 50;
		step = 5;
		start = 5;
		maxScaling = 0;

		
//		double[] budgets= {200.0, 300.0, 400.0, 500.0, 600.0, 700.0, 800.0, 900.0, 1000.0, 1100.0};
		budgets = new double[] {500.0, 1000.0, 1500.0, 2000.0, 2500.0};

		for (double budget : budgets) {
			Experiment.generateSeries(runDirectory, group, dagPath, dags, budget, price, maxHours, step, start, maxScaling, alpha,  taskDilatation, runtimeVariation, delay, distribution, runID);			
		}
		
	}


	



	
}
