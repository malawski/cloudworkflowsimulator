package cws.core.experiment;

import java.io.File;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import cws.core.algorithms.Algorithm;
import cws.core.algorithms.AlgorithmSimulationParams;
import cws.core.algorithms.StorageCacheType;
import cws.core.algorithms.StorageType;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.dag.DAG;
import cws.core.dag.DAGParser;
import cws.core.dag.DAGStats;
import cws.core.dag.Task;
import cws.core.log.WorkflowLog;

public class Experiment {

    /**
     * Runs experiment given its description
     * @param param experiment description
     * @return results
     */

    public ExperimentResult runExperiment(ExperimentDescription param) {

        long startTime = System.nanoTime();

        ExperimentResult result = new ExperimentResult();

        // initialize distributions

        // To get mean m and stddev s, use:
        // sigma = sqrt(log(1+s^2/m^2))
        // mu = log(m)-0.5*log(1+s^2/m^2)
        // For mean = 60 and stddev = 10: mu = 4.080645 sigma = 0.1655264
        // For mean = 20 and stddev = 5: mu = 2.96542, sigma = 0.09975135

        // ContinuousDistribution provisioningDelayDistribution = new LognormalDistr(new
        // java.util.Random(param.getRunID()),4.080645, 0.1655264);
        // ContinuousDistribution deprovisioningDelayDistribution = new LognormalDistr(new
        // java.util.Random(param.getRunID()), 2.96542, 0.09975135);

        // VMFactory.setProvisioningDelayDistribution(provisioningDelayDistribution);
        // VMFactory.setDeprovisioningDelayDistribution(deprovisioningDelayDistribution);

        // TODO(_mequrel_): change to IoC in the future
        CloudSimWrapper cloudsim = new CloudSimWrapper();
        cloudsim.init();

        AlgorithmSimulationParams simulationParams = new AlgorithmSimulationParams();

        // TODO(mequrel): should be parametrized
        simulationParams.setStorageType(StorageType.VOID);
        simulationParams.setStorageCacheType(StorageCacheType.VOID);

        Algorithm algorithm = AlgorithmFactory.createAlgorithm(param, cloudsim, simulationParams);

        algorithm.setGenerateLog(false);

        String fileName = param.getRunDirectory() + File.separator + "output-" + param.getFileName();

        // String fName = param.getRunDirectory() + File.separator + "result-" +
        // param.getAlgorithmName()+"-"+param.getDags()[0]+"x"+param.getDags().length+"d"+param.getDeadline()+"b"+param.getBudget()+"m"+param.getMax_scaling();

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
            sizes.add(sumRuntime(dag));
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
     * @return The total runtime of all tasks in the workflow
     */
    public double sumRuntime(DAG dag) {
        double sum = 0.0;
        for (String taskName : dag.getTasks()) {
            sum += dag.getTaskById(taskName).getSize();
        }
        return sum;
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

    public static void generateSeries(String runDirectory, String group, String dagPath, String[] dags, double budget,
            double price, double N, double step, double start, double max_scaling, double alpha, double taskDilatation,
            double runtimeVariation, double delay, String distribution, int runID) {

        double deadline;

        new File(runDirectory).mkdir();

        // String algorithms[] = {"SPSS", "DPDS", "WADPDS", "MaxMin", "Wide", "Backtrack"};
        String algorithms[] = { "SPSS", "DPDS", "WADPDS" };

        for (double i = start; i <= N; i += step) {
            deadline = 3600 * i; // seconds

            for (String alg : algorithms) {
                ExperimentDescription param = new ExperimentDescription(group, alg, runDirectory, dagPath, dags,
                        deadline, budget, price, max_scaling, alpha, taskDilatation, runtimeVariation, delay,
                        distribution, runID);
                String fileName = "input-" + param.getFileName() + ".properties";
                param.storeProperties(runDirectory + File.separator + fileName);
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

    public static void generateSeries(String runDirectory, String group, String dagPath, String dagName,
            int ensembleSize, String distribution, String algorithms[], double price, double max_scaling, double alpha,
            double taskDilatation, double runtimeVariation, double delay, int runID) {

        // WARNING: These parameters are fixed in the algorithm! Don't change here only!
        double mips = 1;

        new File(runDirectory).mkdir();

        // String algorithms[] = {"SPSS", "DPDS", "WADPDS", "MaxMin", "Wide", "Backtrack"};
        // String algorithms[] = {"SPSS", "DPDS", "WADPDS"};

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
                    t.setSize(t.getSize() * taskDilatation);
                }
            }

            DAGStats s = new DAGStats(dag, mips, price);

            minTime = Math.min(minTime, s.getCriticalPath());
            minCost = Math.min(minCost, s.getMinCost());

            maxTime += s.getCriticalPath();
            maxCost += s.getMinCost();
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
                    ExperimentDescription param = new ExperimentDescription(group, alg, runDirectory, dagPath,
                            dagNames, deadline, budget, price, max_scaling, alpha, taskDilatation, runtimeVariation,
                            delay, distribution, runID);
                    String fileName = "input-" + param.getFileName() + ".properties";
                    param.storeProperties(runDirectory + File.separator + fileName);
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
     * This was the previously used method for running a series of experiments in a single execution.
     * @param group
     * @param dagPath
     * @param dags
     * @param budget
     * @param price
     * @param N
     * @param step
     * @param start
     * @param max_scaling
     * @param alpha
     * @param runID
     */
    public static void runOldSeries(String group, String dagPath, String[] dags, double budget, double price, int N,
            int step, int start, double max_scaling, double alpha, int runID) {

        double deadline;
        ExperimentResult resultsSPSS[] = new ExperimentResult[N + 1];
        ExperimentResult resultsAware[] = new ExperimentResult[N + 1];
        ExperimentResult resultsSimple[] = new ExperimentResult[N + 1];

        for (int i = start; i <= N; i += step) {
            deadline = 3600 * i; // seconds
            Experiment experiment = new Experiment();

            resultsSPSS[i] = experiment.runExperiment(new ExperimentDescription(group, "SPSS", "output", dagPath, dags,
                    deadline, budget, price, max_scaling, alpha, 1.0, 0.0, 0.0, "pareto-sorted", runID));
            resultsAware[i] = experiment.runExperiment(new ExperimentDescription(group, "WADPDS", "output", dagPath,
                    dags, deadline, budget, price, max_scaling, alpha, 1.0, 0.0, 0.0, "pareto-sorted", runID));
            resultsSimple[i] = experiment.runExperiment(new ExperimentDescription(group, "DPDS", "output", dagPath,
                    dags, deadline, budget, price, max_scaling, alpha, 1.0, 0.0, 0.0, "pareto-sorted", runID));
        }

        // write number of dags finished

        StringBuffer outSPSS = new StringBuffer();
        StringBuffer outAware = new StringBuffer();
        StringBuffer outSimple = new StringBuffer();

        for (int i = start; i <= N; i += step) {
            outSPSS.append(resultsSPSS[i].getDeadline() + "  " + resultsSPSS[i].getNumFinishedDAGs() + "\n");
            outAware.append(resultsAware[i].getDeadline() + "  " + resultsAware[i].getNumFinishedDAGs() + "\n");
            outSimple.append(resultsSimple[i].getDeadline() + "  " + resultsSimple[i].getNumFinishedDAGs() + "\n");

        }

        WorkflowLog.stringToFile(outSPSS.toString(), group + dags[0] + "b" + budget + "h" + start + "-" + N + "m"
                + max_scaling + "run" + runID + "-outputSPSS.txt");
        WorkflowLog.stringToFile(outAware.toString(), group + dags[0] + "b" + budget + "h" + start + "-" + N + "m"
                + max_scaling + "run" + runID + "-outputAware.txt");
        WorkflowLog.stringToFile(outSimple.toString(), group + dags[0] + "b" + budget + "h" + start + "-" + N + "m"
                + max_scaling + "run" + runID + "-outputSimple.txt");

        // write priorities of dags finished

        StringBuffer prioritiesSPSS = new StringBuffer();
        StringBuffer prioritiesAware = new StringBuffer();
        StringBuffer prioritiesSimple = new StringBuffer();

        for (int i = start; i <= N; i += step) {
            prioritiesSPSS.append(resultsSPSS[i].formatPriorities());
            prioritiesAware.append(resultsAware[i].formatPriorities());
            prioritiesSimple.append(resultsSimple[i].formatPriorities());
        }

        WorkflowLog.stringToFile(prioritiesSPSS.toString(), group + dags[0] + "b" + budget + "h" + start + "-" + N
                + "m" + max_scaling + "run" + runID + "-prioritiesSPSS.txt");
        WorkflowLog.stringToFile(prioritiesAware.toString(), group + dags[0] + "b" + budget + "h" + start + "-" + N
                + "m" + max_scaling + "run" + runID + "-prioritiesAware.txt");
        WorkflowLog.stringToFile(prioritiesSimple.toString(), group + dags[0] + "b" + budget + "h" + start + "-" + N
                + "m" + max_scaling + "run" + runID + "-prioritiesSimple.txt");

        // write sizes of dags finished

        StringBuffer sizesSPSS = new StringBuffer();
        StringBuffer sizesAware = new StringBuffer();
        StringBuffer sizesSimple = new StringBuffer();

        for (int i = start; i <= N; i += step) {
            sizesSPSS.append(resultsSPSS[i].formatSizes());
            sizesAware.append(resultsAware[i].formatSizes());
            sizesSimple.append(resultsSimple[i].formatSizes());
        }

        WorkflowLog.stringToFile(sizesSPSS.toString(), group + dags[0] + "b" + budget + "h" + start + "-" + N + "m"
                + max_scaling + "run" + runID + "-sizesSPSS.txt");
        WorkflowLog.stringToFile(sizesAware.toString(), group + dags[0] + "b" + budget + "h" + start + "-" + N + "m"
                + max_scaling + "run" + runID + "-sizesAware.txt");
        WorkflowLog.stringToFile(sizesSimple.toString(), group + dags[0] + "b" + budget + "h" + start + "-" + N + "m"
                + max_scaling + "run" + runID + "-sizesSimple.txt");
    }

    /**
     * 
     * Generates a series of experiments, sets runID = 0
     * 
     * @param dagPath
     * @param dags
     * @param budget
     * @param price
     * @param N
     * @param step
     * @param start
     * @param max_scaling
     */

    // public static void generateSeries(String runDirectory, String group, String dagPath, String[] dags, double
    // budget, double price,
    // int N, int step, int start, double max_scaling, double alpha, double taskDilatation) {
    //
    // generateSeries(runDirectory, group, dagPath, dags, budget, price, N, step, start, max_scaling, alpha,
    // taskDilatation, 0);
    // }

    /**
     * 
     * Generates a series constructing dags array by repeating the same DAG file numDAGs times.
     * 
     * @param dagPath
     * @param dagName
     * @param budget
     * @param price
     * @param numDAGs
     * @param N
     * @param step
     * @param start
     * @param max_scaling
     * @param runID
     */
    public static void generateSeries(String runDirectory, String group, String dagPath, String dagName, double budget,
            double price, int numDAGs, double N, double step, double start, double max_scaling, double alpha,
            double taskDilatation, double runtimeVariation, double delay, String distribution, int runID) {

        String[] dags = new String[numDAGs];

        for (int i = 0; i < numDAGs; i++)
            dags[i] = dagName;

        generateSeries(runDirectory, group, dagPath, dags, budget, price, N, step, start, max_scaling, alpha,
                taskDilatation, runtimeVariation, delay, distribution, runID);

    }

    /**
     * Generates a series constructing dags array by repeating the same DAG file numDAGs times.
     * Sets runID to 0.
     * 
     * @param dagPath
     * @param dagName
     * @param budget
     * @param price
     * @param numDAGs
     * @param N
     * @param step
     * @param start
     * @param max_scaling
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
