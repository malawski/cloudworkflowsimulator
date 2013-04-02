package cws.core.experiment;

import java.util.Random;

/**
 * Generates series of ensembles consisting of workflows form workflow generator
 * https://confluence.pegasus.isi.edu/display/pegasus/WorkflowGenerator
 * 
 * From each workflow type DAGs of Pareto - distributed sizes are selected.
 * 
 * Tasks are dilated by factors of 2,4,8,16,
 * 
 * @author malawski
 * 
 */

public class GenerateExperimentParetoDilated {

    /******************************
     * 
     * Tests with max scaling = 0.0
     * 
     ******************************/
    public static void main(String[] args) {

        String dagPath;
        String dagName;
        double price = 1.0;
        double maxScaling = 2.0;
        String group = "pareto-dilated-nodelays";
        double alpha = 0.7;
        String[] dags;
        double[] budgets;
        String runDirectory = "";
        int runID = 0;
        int taskDilatation = 1;
        double runtimeVariation = 0.0;
        double delay = 0.0;
        String distribution = "pareto-sorted";

        int maxHours;
        int stepHours;
        int startHours;

        int numRunIDs = 2;

        for (taskDilatation = 1; taskDilatation <= 1024; taskDilatation *= 2) {

            for (runID = 0; runID < numRunIDs; runID++) {

                runDirectory = String.format("run-t%04d-%03d-%s", taskDilatation, runID, group);

                dagPath = "../projects/pegasus/Montage/";
                dagName = "MONTAGE";

                dags = DAGListGenerator.generateDAGListPareto(new Random(runID), dagName, 100);
                maxHours = 20 * taskDilatation;
                stepHours = 1 * taskDilatation;
                startHours = 1 * taskDilatation;
                maxScaling = 0 * taskDilatation;

                // budgets= new double[] {10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0, 100.0};
                budgets = new double[] { 20.0, 30.0, 50.0, 60.0, 80.0 };

                for (double budget : budgets) {
                    Experiment.generateSeries(runDirectory, group, dagPath, dags, budget * taskDilatation, price,
                            maxHours, stepHours, startHours, maxScaling, alpha, taskDilatation, runtimeVariation,
                            delay, distribution, runID);
                }

                dagPath = "../projects/pegasus/CyberShake/";
                dagName = "CYBERSHAKE";

                dags = DAGListGenerator.generateDAGListPareto(new Random(runID), dagName, 100);

                maxHours = 20 * taskDilatation;
                stepHours = 1 * taskDilatation;
                startHours = 1 * taskDilatation;
                maxScaling = 0;

                // budgets= {10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 80.0, 100.0, 120.0, 140.0};
                budgets = new double[] { 30.0, 50.0, 80.0, 100.0, 140.0 };

                for (double budget : budgets) {
                    Experiment.generateSeries(runDirectory, group, dagPath, dags, budget * taskDilatation, price,
                            maxHours, stepHours, startHours, maxScaling, alpha, taskDilatation, runtimeVariation,
                            delay, distribution, runID);
                }

                // dagPath = "../projects/pegasus/LIGO/";
                // dagName = "LIGO";
                //
                // dags = DAGListGenerator.generateDAGListPareto(new Random(runID), dagName, 100);
                //
                // maxHours = 40 * taskDilatation;
                // stepHours = 1 * taskDilatation;
                // startHours = 1 * taskDilatation;
                // maxScaling = 0;
                //
                //
                // // budgets= new double[] {200.0, 400.0, 600.0, 800.0, 1000.0, 1200.0, 1400.0, 1600.0, 1800.0,
                // 2000.0};
                // budgets= new double[] {400.0, 600.0, 800.0, 1000.0, 1200.0};
                //
                // for (double budget : budgets) {
                // Experiment.generateSeries(runDirectory, group, dagPath, dags, budget * taskDilatation, price,
                // maxHours, stepHours, startHours, maxScaling, alpha, taskDilatation, runID);
                // }
                //
                //
                // dagPath = "../projects/pegasus/Genome/";
                // dagName = "GENOME";
                //
                // dags = DAGListGenerator.generateDAGListPareto(new Random(runID), dagName, 100);
                //
                // maxHours = 1500 * taskDilatation;
                // stepHours = 100 * taskDilatation;
                // startHours = 100 * taskDilatation;
                // maxScaling = 0;
                //
                //
                // // budgets= {2000.0, 4000.0, 6000.0, 8000.0, 10000.0, 12000.0, 14000.0, 16000.0, 18000.0, 20000.0};
                // budgets= new double[] {4000.0, 6000.0, 8000.0, 10000.0, 12000.0};
                //
                //
                // for (double budget : budgets) {
                // Experiment.generateSeries(runDirectory, group, dagPath, dags, budget * taskDilatation, price,
                // maxHours, stepHours, startHours, maxScaling, alpha, taskDilatation, runID);
                // }
                //
                //
                // dagPath = "../projects/pegasus/SIPHT/";
                // dagName = "SIPHT";
                //
                // dags = DAGListGenerator.generateDAGListPareto(new Random(runID), dagName, 100);
                //
                // maxHours = 50 * taskDilatation;
                // stepHours = 5 * taskDilatation;
                // startHours = 5 * taskDilatation;
                // maxScaling = 0;
                //
                //
                // // budgets= new double[] {200.0, 300.0, 400.0, 500.0, 600.0, 700.0, 800.0, 900.0, 1000.0, 1100.0};
                // budgets= new double[] {200.0, 400.0, 600.0, 800.0, 1000.0};
                //
                // for (double budget : budgets) {
                // Experiment.generateSeries(runDirectory, group, dagPath, dags, budget * taskDilatation, price,
                // maxHours, stepHours, startHours, maxScaling, alpha, taskDilatation, runID);
                // }

            }

        }

    }

}
