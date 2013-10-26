package cws.core.experiment.generators;

import cws.core.experiment.Experiment;

/**
 * Generates series of ensembles consisting of workflows form workflow generator
 * https://confluence.pegasus.isi.edu/display/pegasus/WorkflowGenerator
 * 
 * From each workflow type DAGs of Pareto - distributed sizes are selected.
 */

public class GenerateExperimentVariation extends AbstractGenerateExperiment {

    /******************************
     * Tests with max scaling = 0.0
     ******************************/
    @Override
    protected void doGenerate() {
        maxScaling = 0.0;
        group = "variations-50";
        runDirectory = "";
        String[] distributions = { "uniform_unsorted", "uniform_sorted", "pareto_unsorted", "pareto_sorted", "constant" };
        int ensemble_size = 50;

        double runtimeVariations[] = { 0.00, 0.01, 0.02, 0.05, 0.10, 0.20, 0.50 };

        String algorithms[] = { "SPSS", "DPDS", "WADPDS" };

        int numRunIDs = 10;

        for (String distribution : distributions) {
            for (runID = 0; runID < numRunIDs; runID++) {
                for (double runtimeVariation : runtimeVariations) {
                    runDirectory = String.format("run-%03d-%s-v%2.2f-%s", runID, distribution, runtimeVariation, group);

                    dagPath = dagPathPrefix + "Montage/";
                    dagName = "MONTAGE";
                    Experiment.generateSeries(runDirectory, group, dagPath, dagName, ensemble_size, distribution,
                            algorithms, price, maxScaling, alpha, taskDilatation, runtimeVariation, delay, runID);

                    dagPath = dagPathPrefix + "CyberShake/";
                    dagName = "CYBERSHAKE";
                    Experiment.generateSeries(runDirectory, group, dagPath, dagName, ensemble_size, distribution,
                            algorithms, price, maxScaling, alpha, taskDilatation, runtimeVariation, delay, runID);

                    dagPath = dagPathPrefix + "LIGO/";
                    dagName = "LIGO";
                    Experiment.generateSeries(runDirectory, group, dagPath, dagName, ensemble_size, distribution,
                            algorithms, price, maxScaling, alpha, taskDilatation, runtimeVariation, delay, runID);

                    dagPath = dagPathPrefix + "Genome/";
                    dagName = "GENOME";
                    Experiment.generateSeries(runDirectory, group, dagPath, dagName, ensemble_size, distribution,
                            algorithms, price, maxScaling, alpha, taskDilatation, runtimeVariation, delay, runID);

                    dagPath = dagPathPrefix + "SIPHT/";
                    dagName = "SIPHT";
                    Experiment.generateSeries(runDirectory, group, dagPath, dagName, ensemble_size, distribution,
                            algorithms, price, maxScaling, alpha, taskDilatation, runtimeVariation, delay, runID);
                }
            }
        }
    }
}
