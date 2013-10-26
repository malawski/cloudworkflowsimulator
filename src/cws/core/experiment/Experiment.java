package cws.core.experiment;

import java.io.File;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import cws.core.algorithms.Algorithm;
import cws.core.algorithms.AlgorithmSimulationParams;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.dag.DAG;
import cws.core.dag.DAGParser;
import cws.core.dag.DAGStats;
import cws.core.dag.Task;
import cws.core.log.WorkflowLog;

public class Experiment {

    /**
     * Runs experiment given its description.
     * @param param The experiment description.
     * @return results
     */
    public ExperimentResult runExperiment(ExperimentDescription param) {
        return runExperiment(param, new CloudSimWrapper());
    }

    /**
     * Runs experiment given its description.
     * @param param The experiment description.
     * @param cloudsim The cloudsim instance.
     * @return results
     */
    public ExperimentResult runExperiment(ExperimentDescription param, CloudSimWrapper cloudsim) {
        long startTime = System.nanoTime();

        ExperimentResult result = new ExperimentResult();

        cloudsim.init();

        Algorithm algorithm = AlgorithmFactory.createAlgorithm(param, cloudsim);

        algorithm.setGenerateLog(false);

        String fileName = param.getRunDirectory() + File.separator + "output-" + param.getFileName();

        long simulationStartTime = System.nanoTime();
        algorithm.simulate(fileName);

        result.setInitWallTime(simulationStartTime - startTime);
        result.setPlanningWallTime(algorithm.getPlanningnWallTime());
        result.setSimulationWallTime(algorithm.getSimulationWallTime());

        result.setBudget(param.getBudget());
        result.setDeadline(param.getDeadline());
        result.setCost(algorithm.getActualCost());
        result.setAlgorithm(param.getAlgorithmName());

        List<Integer> priorities = new LinkedList<Integer>();
        List<Double> sizes = new LinkedList<Double>();

        int finished = algorithm.numCompletedDAGs();
        for (DAG dag : algorithm.getCompletedDAGs()) {
            sizes.add(dag.getRuntimeSum(algorithm.getStorageManager()));
        }
        priorities = algorithm.completedDAGPriorities();
        result.setNumFinishedDAGs(finished);
        result.setPriorities(priorities);
        result.setSizes(sizes);

        result.setActualFinishTime(algorithm.getActualDagFinishTime());
        result.setScoreBitString(algorithm.getScoreBitString());

        return result;
    }

    /**
     * Generates a series of experiments for varying deadlines
     * @param runDirectory the directory with input and output files for this series
     * @param group prefix to prepend to generated output files
     * @param dagPath path to dags
     * @param dags array of file names
     * @param budget budget in $
     * @param price VM hour price in $
     * @param N max deadline in hours
     * @param step step between deadlines in hours
     * @param start min deadline in hours
     * @param max_scaling max autoscaling factor
     * @param runID id of this series
     */
    public static void generateDeadlineBasedSeries(String runDirectory, String group, String dagPath, String[] dags,
            double budget, double price, double N, double step, double start, double max_scaling, double alpha,
            double taskDilatation, double runtimeVariation, double delay, String distribution, int runID) {
        new File(runDirectory).mkdir();

        String algorithms[] = { "SPSS", "DPDS", "WADPDS" };
        for (double i = start; i <= N; i += step) {
            double deadline = 3600 * i; // seconds
            for (String alg : algorithms) {
                for (AlgorithmSimulationParams algorithmParams : AlgorithmSimulationParams.getAllSimulationParams()) {
                    ExperimentDescription param = new ExperimentDescription(group, alg, runDirectory, dagPath, dags,
                            deadline, budget, price, max_scaling, alpha, taskDilatation, runtimeVariation, delay,
                            distribution, runID);
                    param.setSimulationParams(algorithmParams);
                    String fileName = "input-" + param.getFileName() + ".properties";
                    param.storeProperties(runDirectory + File.separator + fileName);
                }
            }
        }
    }

    /**
     * Generates a series of experiments , calculates deadlines and budgets based on estimated cost and critical path of
     * ensemble
     * @param runDirectory the directory with input and output files for this series
     * @param group prefix to prepend to generated output files
     * @param dagPath path to dags
     * @param dags array of file names
     * @param price VM hour price in $
     * @param max_scaling max autoscaling factor
     * @param runID id of this series
     */
    public static void generateSeriesWithCriticalPath(String runDirectory, String group, String dagPath,
            String dagName, int ensembleSize, String distribution, String algorithms[], double price,
            double max_scaling, double alpha, double taskDilatation, double runtimeVariation, double delay, int runID) {
        new File(runDirectory).mkdir();

        String[] dagNames = null;

        if ("uniform_unsorted".equals(distribution)) {
            dagNames = DAGListGenerator.generateDAGListUniformUnsorted(new Random(runID), dagName, ensembleSize);
        } else if ("uniform_sorted".equals(distribution)) {
            dagNames = DAGListGenerator.generateDAGListUniform(new Random(runID), dagName, ensembleSize);
        } else if ("pareto_unsorted".equals(distribution)) {
            dagNames = DAGListGenerator.generateDAGListParetoUnsorted(new Random(runID), dagName, ensembleSize);
        } else if ("pareto_sorted".equals(distribution)) {
            dagNames = DAGListGenerator.generateDAGListPareto(new Random(runID), dagName, ensembleSize);
        } else if ("constant".equals(distribution)) {
            dagNames = DAGListGenerator.generateDAGListConstant(new Random(runID), dagName, ensembleSize);
        } else if (distribution.startsWith("fixed")) {
            int size = Integer.parseInt(distribution.substring(5));
            dagNames = DAGListGenerator.generateDAGListConstant(dagName, size, ensembleSize);
        } else {
            System.err.println("Unrecognized distribution: " + distribution);
            System.exit(1);
        }

        double minTime = Double.MAX_VALUE;
        double minCost = Double.MAX_VALUE;
        double maxCost = 0.0;
        double maxTime = 0.0;

        List<DAG> dags = new ArrayList<DAG>();
        for (String name : dagNames) {
            String fileName = dagPath + File.separator + name;
            // System.out.println(fileName);
            DAG dag = DAGParser.parseDAG(new File(fileName));
            dags.add(dag);

            if (taskDilatation > 1.0) {
                for (String tid : dag.getTasks()) {
                    Task t = dag.getTaskById(tid);
                    t.scaleSize(taskDilatation);
                }
            }

            // TODO(bryk): introduce storage manager here
            DAGStats stats = new DAGStats(dag, null);

            minTime = Math.min(minTime, stats.getCriticalPath());
            minCost = Math.min(minCost, stats.getMinCost());

            maxTime += stats.getCriticalPath();
            maxCost += stats.getMinCost();
        }

        int nbudgets = 10;
        int ndeadlines = 10;

        double minBudget = Math.ceil(minCost);
        double maxBudget = Math.ceil(maxCost);
        double budgetStep = (maxBudget - minBudget) / (nbudgets - 1);

        double minDeadline = Math.ceil(minTime);
        double maxDeadline = Math.ceil(maxTime);
        double deadlineStep = (maxDeadline - minDeadline) / (ndeadlines - 1);

        System.out.printf("application = %s, distribution = %s\n", dagName, distribution);
        System.out.printf("budget = %f %f %f\n", minBudget, maxBudget, budgetStep);
        System.out.printf("deadline = %f %f %f\n", minDeadline, maxDeadline, deadlineStep);

        // we add 0.00001 as epsilon to avoid rounding errors
        for (double budget = minBudget; budget <= maxBudget + 0.00001; budget += budgetStep) {
            for (double deadline = minDeadline; deadline <= maxDeadline + 0.00001; deadline += deadlineStep) {
                for (String alg : algorithms) {
                    for (AlgorithmSimulationParams algorithmParams : AlgorithmSimulationParams.getAllSimulationParams()) {
                        ExperimentDescription param = new ExperimentDescription(group, alg, runDirectory, dagPath,
                                dagNames, deadline, budget, price, max_scaling, alpha, taskDilatation,
                                runtimeVariation, delay, distribution, runID);
                        param.setSimulationParams(algorithmParams);
                        String fileName = "input-" + param.getFileName() + ".properties";
                        param.storeProperties(runDirectory + File.separator + fileName);
                    }
                }
            }
        }
    }

    /**
     * Runs a single experiment given its description in property file.
     * Result is saved in corresponding result file in the same directory.
     * @param args
     */
    public static void main(String[] args) {
        Experiment experiment = new Experiment();
        ExperimentDescription param = new ExperimentDescription(args[0]);
        ExperimentResult result = experiment.runExperiment(param);
        System.out.println(result.formatResult());

        String fileName = "result-" + param.getFileName() + "-result.txt";

        WorkflowLog.stringToFile(result.formatResult(), param.getRunDirectory() + File.separator + fileName);
    }

    /**
     * Generates a series constructing dags array by repeating the same DAG file numDAGs times.
     */
    public static void generateSeries(String runDirectory, String group, String dagPath, String dagName, double budget,
            double price, int numDAGs, double N, double step, double start, double max_scaling, double alpha,
            double taskDilatation, double runtimeVariation, double delay, String distribution, int runID) {

        String[] dags = new String[numDAGs];

        for (int i = 0; i < numDAGs; i++)
            dags[i] = dagName;

        generateDeadlineBasedSeries(runDirectory, group, dagPath, dags, budget, price, N, step, start, max_scaling,
                alpha, taskDilatation, runtimeVariation, delay, distribution, runID);

    }

    /**
     * Generates a series constructing dags array by repeating the same DAG file numDAGs times.
     * Sets runID to 0.
     */
    public static void generateConstantSeries(String runDirectory, String group, String dagPath, String dagName,
            double budget, double price, int numDAGs, double N, double step, double start, double max_scaling,
            double taskDilatation, double runtimeVariation, double delay, double alpha) {
        generateSeries(runDirectory, group, dagPath, dagName, budget, price, numDAGs, N, step, start, max_scaling,
                alpha, taskDilatation, runtimeVariation, delay, "constant", 0);
    }

    /**
     * Generates a series "runs" times, increasing runID from 0 to runs-1
     * @param runs number of runs
     */
    public static void generateSeriesRepeat(String runDirectory, String group, String dagPath, String dagName,
            double budget, double price, int numDAGs, double N, double step, double start, double max_scaling,
            double alpha, double taskDilatation, double runtimeVariation, double delay, int runs) {

        for (int i = 0; i < runs; i++) {
            generateSeries(runDirectory, group, dagPath, dagName, budget, price, numDAGs, N, step, start, max_scaling,
                    alpha, taskDilatation, runtimeVariation, delay, "constant", i);
        }
    }
}
