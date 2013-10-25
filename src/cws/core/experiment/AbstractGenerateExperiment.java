package cws.core.experiment;

/**
 * Tests series of ensembles consisting of workflows form workflow generator
 * https://confluence.pegasus.isi.edu/display/pegasus/WorkflowGenerator
 */
public abstract class AbstractGenerateExperiment {
    protected String dagPathPrefix;
    protected String dagPath;
    protected String dagName;
    protected double price;
    protected double maxScaling;
    protected String group;
    protected double alpha;
    protected String runDirectory;
    protected int maxHours;
    protected int step;
    protected int start;
    protected int runID;
    protected double taskDilatation;
    protected double runtimeVariation;
    protected double delay;
    protected String distribution;

    public void generate(String[] args) {
        dagPathPrefix = args[0];
        if (args.length != 1) {
            System.err.println("Required param: dagPathPrefix");
            System.exit(-1);
        }
    }

    protected abstract void doGenerate();

    protected void generateSeries(double[] budgets, String dags[]) {
        for (double budget : budgets) {
            Experiment.generateSeries(runDirectory, group, dagPath, dags, budget, price, maxHours, step, start,
                    maxScaling, alpha, taskDilatation, runtimeVariation, delay, distribution, runID);
        }
    }
}
