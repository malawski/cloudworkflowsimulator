package cws.core.experiment.generators;

import cws.core.experiment.Experiment;

/**
 * Tests series of ensembles consisting of workflows form workflow generator.
 * https://confluence.pegasus.isi.edu/display/pegasus/WorkflowGenerator
 * 
 * Abstract class extended by experiment input generators.
 */
public abstract class AbstractGenerateExperiment {
    protected String dagPathPrefix;
    protected String dagPath;
    protected String dagName;
    protected String group;
    protected String runDirectory;
    protected String distribution;
    protected double price = 1.0;
    protected double maxScaling;
    protected double taskDilatation = 1.0;
    protected double runtimeVariation;
    protected double delay = 0;
    protected double alpha = 0.7;
    protected double stepHours;
    protected double startHours;
    protected double maxHours;
    protected int runID = 0;

    /**
     * Generates experiment prams and saves it to file. DAG path prefix should be provided as program's param.
     * @param args Program's params.
     */
    public void generate(String[] args) {
        if (args.length != 1) {
            System.err.println("Required param: dagPathPrefix");
            System.exit(-1);
        } else {
            dagPathPrefix = args[0];
            doGenerate();
        }
    }

    /**
     * Should be overriden and actually generate experiment params.
     */
    protected abstract void doGenerate();

    /**
     * Generates series of params based on the given budget, dags and internal properties.
     * @param budgets The array of budgets.
     * @param dags The array of DAGs.
     */
    protected void generateSeries(double[] budgets, String dags[]) {
        for (double budget : budgets) {
            Experiment.generateDeadlineBasedSeries(runDirectory, group, dagPath, dags, budget * taskDilatation, price,
                    maxHours, stepHours, startHours, maxScaling, alpha, taskDilatation, runtimeVariation, delay,
                    distribution, runID);
        }
    }
}
