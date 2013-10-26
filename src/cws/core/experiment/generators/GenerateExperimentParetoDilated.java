package cws.core.experiment.generators;

import java.util.Random;

import cws.core.experiment.DAGListGenerator;

/**
 * Generates series of ensembles consisting of workflows form workflow generator
 * https://confluence.pegasus.isi.edu/display/pegasus/WorkflowGenerator
 * 
 * From each workflow type DAGs of Pareto - distributed sizes are selected.
 * 
 * Tasks are dilated by factors of 2,4,8,16,...
 */

public class GenerateExperimentParetoDilated extends AbstractGenerateExperiment {

    public static void main(String[] args) {
        new GenerateExperimentParetoDilated().generate(args);
    }

    /******************************
     * Tests with max scaling = 0.0
     ******************************/
    @Override
    protected void doGenerate() {
        maxScaling = 2.0;
        group = "pareto-dilated-nodelays";
        runDirectory = "";
        runtimeVariation = 0.0;
        distribution = "pareto-sorted";

        int numRunIDs = 2;
        for (taskDilatation = 1; taskDilatation <= 1024; taskDilatation *= 2) {
            for (runID = 0; runID < numRunIDs; runID++) {
                runDirectory = String.format("run-t%04d-%03d-%s", (int) taskDilatation, runID, group);

                dagPath = dagPathPrefix + "Montage/";
                dagName = "MONTAGE";

                String[] dags = DAGListGenerator.generateDAGListPareto(new Random(runID), dagName, 100);
                maxHours = 20 * taskDilatation;
                stepHours = 1 * taskDilatation;
                startHours = 1 * taskDilatation;
                maxScaling = 0 * taskDilatation;

                // budgets= new double[] {10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0, 100.0};
                double[] budgets = new double[] { 20.0, 30.0, 50.0, 60.0, 80.0 };

                generateSeries(budgets, dags);

                dagPath = dagPathPrefix + "CyberShake/";
                dagName = "CYBERSHAKE";

                dags = DAGListGenerator.generateDAGListPareto(new Random(runID), dagName, 100);

                maxHours = 20 * taskDilatation;
                stepHours = 1 * taskDilatation;
                startHours = 1 * taskDilatation;
                maxScaling = 0;

                // budgets= {10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 80.0, 100.0, 120.0, 140.0};
                budgets = new double[] { 30.0, 50.0, 80.0, 100.0, 140.0 };

                generateSeries(budgets, dags);
            }
        }
    }
}
