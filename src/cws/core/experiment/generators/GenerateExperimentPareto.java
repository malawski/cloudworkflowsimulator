package cws.core.experiment.generators;

import java.util.Random;

import cws.core.experiment.DAGListGenerator;

/**
 * Generates series of ensembles consisting of workflows form workflow generator
 * https://confluence.pegasus.isi.edu/display/pegasus/WorkflowGenerator
 * 
 * From each workflow type DAGs of Pareto - distributed sizes are selected.
 */

public class GenerateExperimentPareto extends AbstractGenerateExperiment {
    public static void main(String[] args) {
        new GenerateExperimentPareto().generate(args);
    }

    /******************************
     * Tests with max scaling = 0.0
     ******************************/
    @Override
    protected void doGenerate() {
        maxScaling = 2.0;
        group = "pareto-nodelays-logs";
        runDirectory = "run-01-pareto";
        runtimeVariation = 0.0;
        distribution = "pareto-sorted";

        for (runID = 0; runID < 1; runID++) {
            runDirectory = String.format("run-%03d-%s", runID, group);

            dagPath = dagPathPrefix + "Montage/";
            dagName = "MONTAGE";

            String[] dags = DAGListGenerator.generateDAGListPareto(new Random(runID), dagName, 100);
            maxHours = 20;
            stepHours = 1;
            startHours = 1;
            maxScaling = 0;

            // budgets= new double[] {10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0, 100.0};
            double[] budgets = new double[] { 20.0, 30.0, 50.0, 60.0, 80.0 };

            generateSeries(budgets, dags);

            dagPath = dagPathPrefix + "CyberShake/";
            dagName = "CYBERSHAKE";

            dags = DAGListGenerator.generateDAGListPareto(new Random(runID), dagName, 100);

            maxHours = 20;
            stepHours = 1;
            startHours = 1;
            maxScaling = 0;

            // budgets= {10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 80.0, 100.0, 120.0, 140.0};
            budgets = new double[] { 30.0, 50.0, 80.0, 100.0, 140.0 };

            generateSeries(budgets, dags);

            dagPath = dagPathPrefix + "LIGO/";
            dagName = "LIGO";

            dags = DAGListGenerator.generateDAGListPareto(new Random(runID), dagName, 100);

            maxHours = 40;
            stepHours = 1;
            startHours = 1;
            maxScaling = 0;

            // budgets= new double[] {200.0, 400.0, 600.0, 800.0, 1000.0, 1200.0, 1400.0, 1600.0, 1800.0, 2000.0};
            budgets = new double[] { 400.0, 600.0, 800.0, 1000.0, 1200.0 };

            generateSeries(budgets, dags);

            dagPath = dagPathPrefix + "Genome/";
            dagName = "GENOME";

            dags = DAGListGenerator.generateDAGListPareto(new Random(runID), dagName, 100);

            maxHours = 1500;
            stepHours = 100;
            startHours = 100;
            maxScaling = 0;

            // budgets= {2000.0, 4000.0, 6000.0, 8000.0, 10000.0, 12000.0, 14000.0, 16000.0, 18000.0, 20000.0};
            budgets = new double[] { 4000.0, 6000.0, 8000.0, 10000.0, 12000.0 };

            generateSeries(budgets, dags);
            dagPath = dagPathPrefix + "SIPHT/";
            dagName = "SIPHT";

            dags = DAGListGenerator.generateDAGListPareto(new Random(runID), dagName, 100);

            maxHours = 50;
            stepHours = 5;
            startHours = 5;
            maxScaling = 0;

            // budgets= new double[] {200.0, 300.0, 400.0, 500.0, 600.0, 700.0, 800.0, 900.0, 1000.0, 1100.0};
            budgets = new double[] { 200.0, 400.0, 600.0, 800.0, 1000.0 };

            generateSeries(budgets, dags);
        }
    }
}
