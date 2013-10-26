package cws.core.experiment.generators;

import cws.core.experiment.Experiment;

/**
 * Tests series of ensembles consisting of workflows form workflow generator.
 * https://confluence.pegasus.isi.edu/display/pegasus/WorkflowGenerator
 * 
 * TODO(bryk): Docuemnt it.
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
     * TODO(bryk): Document it.
     * @param args
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
     * TODO(bryk): Document it.
     */
    protected abstract void doGenerate();

    /**
     * TODO(bryk): Document it.
     * @param budgets
     * @param dags
     */
    protected void generateSeries(double[] budgets, String dags[]) {
        for (double budget : budgets) {
            Experiment.generateDeadlineBasedSeries(runDirectory, group, dagPath, dags, budget * taskDilatation, price,
                    maxHours, stepHours, startHours, maxScaling, alpha, taskDilatation, runtimeVariation, delay,
                    distribution, runID);
        }
    }
}
