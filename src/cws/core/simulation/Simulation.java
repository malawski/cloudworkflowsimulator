package cws.core.simulation;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.io.IOUtils;
import org.cloudbus.cloudsim.Log;

import cws.core.algorithms.*;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.core.VMTypeBuilder;
import cws.core.dag.DAG;
import cws.core.dag.DAGListGenerator;
import cws.core.dag.DAGParser;
import cws.core.dag.DAGStats;
import cws.core.dag.Task;
import cws.core.engine.Environment;
import cws.core.engine.EnvironmentFactory;
import cws.core.exception.IllegalCWSArgumentException;
import cws.core.provisioner.VMFactory;
import cws.core.storage.StorageManagerStatistics;
import cws.core.storage.global.GlobalStorageParams;

public class Simulation {
    /**
     * The number of workflows an ensemble is comprised of.
     */
    private static final String DEFAULT_ENSEMBLE_SIZE = "20";

    /**
     * The scaling factor for jobs' runtimes.
     */
    private static final String DEFAULT_SCALING_FACTOR = "1.0";

    /**
     * Storage cache type. Allowed values: void, global.
     */
    private static final String DEFAULT_STORAGE_CACHE = "void";

    /**
     * Whether to enable simulation logging. It is needed for validation and gantt graphs generation, but can decrease
     * performance especially if logs are dumped to stdout.
     */
    private static final String DEFAULT_ENABLE_LOGGING = "true";

    /**
     * Number of budgets generated. It is ignored when budget is explicitly set.
     */
    private static final String DEFAULT_N_BUDGETS = "10";

    /**
     * Number of deadlines generated. It is ignored when deadline is explicitly set.
     */
    private static final String DEFAULT_N_DEADLINES = "10";

    /**
     * How many times more can the number of VMs be increased? 1.0 means 0%, 2.0 means 100%, etc..
     */
    private static final String DEFAULT_MAX_SCALING = "1.0";

    /**
     * The algorithm alpha parameter.
     */
    private static final String DEFAULT_ALPHA = "0.7";

    /**
     * Whether the algorithm should be aware of the underlying storage. I.e. during cumputing runtime estimations.
     */
    private static final String DEFAULT_IS_STORAGE_AWARE = "true";

    public static Options buildOptions() {
        Options options = new Options();

        Option seed = new Option("s", "seed", true, "Random number generator seed, defaults to current time in milis");
        seed.setArgName("SEED");
        options.addOption(seed);

        Option application = new Option("app", "application", true, "(required) Application name");
        application.setRequired(true);
        application.setArgName("APP");
        options.addOption(application);

        Option inputdir = new Option("id", "input-dir", true, "(required) Input dir");
        inputdir.setRequired(true);
        inputdir.setArgName("DIR");
        options.addOption(inputdir);

        Option outputfile = new Option("of", "output-file", true, "(required) Output file");
        outputfile.setRequired(true);
        outputfile.setArgName("FILE");
        options.addOption(outputfile);

        Option distribution = new Option("dst", "distribution", true, "(required) Distribution");
        distribution.setRequired(true);
        distribution.setArgName("DIST");
        options.addOption(distribution);

        Option ensembleSize = new Option("es", "ensemble-size", true, "Ensemble size, defaults to "
                + DEFAULT_ENSEMBLE_SIZE);
        ensembleSize.setArgName("SIZE");
        options.addOption(ensembleSize);

        Option algorithm = new Option("alg", "algorithm", true, "(required) Algorithm");
        algorithm.setRequired(true);
        algorithm.setArgName("ALGO");
        options.addOption(algorithm);

        Option scalingFactor = new Option("sf", "scaling-factor", true, "Scaling factor, defaults to "
                + DEFAULT_SCALING_FACTOR);
        scalingFactor.setArgName("FACTOR");
        options.addOption(scalingFactor);

        Option storageCache = new Option("sc", "storage-cache", true, "Storage cache, defaults to "
                + DEFAULT_STORAGE_CACHE);
        storageCache.setArgName("CACHE");
        options.addOption(storageCache);

        Option storageManager = new Option("sm", "storage-manager", true, "(required) Storage manager ");
        storageManager.setRequired(true);
        storageManager.setArgName("MRG");
        options.addOption(storageManager);

        Option enableLogging = new Option("el", "enable-logging", true, "Whether to enable logging, defaults to "
                + DEFAULT_ENABLE_LOGGING);
        enableLogging.setArgName("BOOL");
        options.addOption(enableLogging);

        Option deadline = new Option("d", "deadline", true, "Optional deadline, which overrides max and min deadlines");
        deadline.setArgName("DEADLINE");
        options.addOption(deadline);

        Option budget = new Option("b", "budget", true, "Optional budget, which overrides max and min budgets");
        budget.setArgName("DEADLINE");
        options.addOption(budget);

        Option nBudgets = new Option("nb", "n-budgets", true, "Optional number of generated budgets, defaults to "
                + DEFAULT_N_BUDGETS);
        nBudgets.setArgName("N");
        options.addOption(nBudgets);

        Option nDeadlines = new Option("nd", "n-deadlines", true,
                "Optional number of generated deadlines, defaults to " + DEFAULT_N_DEADLINES);
        nDeadlines.setArgName("N");
        options.addOption(nDeadlines);

        Option maxScaling = new Option("ms", "max-scaling", true,
                "Optional maximum VM number scaling factor, defaults to " + DEFAULT_MAX_SCALING);
        maxScaling.setArgName("FLOAT");
        options.addOption(maxScaling);

        Option alpha = new Option("alp", "alpha", true, "Optional alpha factor, defaults to " + DEFAULT_ALPHA);
        alpha.setArgName("FLOAT");
        options.addOption(alpha);

        Option isStorageAware = new Option("sa", "storage-aware", true,
                "Whether the algorithms should be storage aware, defaults to " + DEFAULT_IS_STORAGE_AWARE);
        isStorageAware.setArgName("BOOL");
        options.addOption(isStorageAware);

        GlobalStorageParams.buildCliOptions(options);
        VMFactory.buildCliOptions(options);
        return options;
    }

    private static void printUsage(Options options, String reason) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.setWidth(120);
        formatter.printHelp(Simulation.class.getName(), "", options, reason);
        System.exit(1);
    }

    public static void main(String[] args) {
        Options options = buildOptions();
        CommandLine cmd = null;
        try {
            CommandLineParser parser = new PosixParser();
            cmd = parser.parse(options, args);
        } catch (ParseException exp) {
            printUsage(options, exp.getMessage());
        }
        Simulation testRun = new Simulation();
        try {
            testRun.runTest(cmd);
        } catch (IllegalCWSArgumentException e) {
            printUsage(options, e.getMessage());
        }
    }

    public void runTest(CommandLine args) {
        // Arguments with no defaults
        String algorithmName = args.getOptionValue("algorithm");
        String application = args.getOptionValue("application");
        File inputdir = new File(args.getOptionValue("input-dir"));
        File outputfile = new File(args.getOptionValue("output-file"));
        String distribution = args.getOptionValue("distribution");
        String storageManagerType = args.getOptionValue("storage-manager");

        // Arguments with defaults
        int ensembleSize = Integer.parseInt(args.getOptionValue("ensemble-size", DEFAULT_ENSEMBLE_SIZE));
        double scalingFactor = Double.parseDouble(args.getOptionValue("scaling-factor", DEFAULT_SCALING_FACTOR));
        long seed = Long.parseLong(args.getOptionValue("seed", System.currentTimeMillis() + ""));
        String storageCacheType = args.getOptionValue("storage-cache", DEFAULT_STORAGE_CACHE);
        boolean enableLogging = Boolean.valueOf(args.getOptionValue("enable-logging", DEFAULT_ENABLE_LOGGING));
        int nbudgets = Integer.parseInt(args.getOptionValue("n-budgets", DEFAULT_N_BUDGETS));
        int ndeadlines = Integer.parseInt(args.getOptionValue("n-deadlines", DEFAULT_N_DEADLINES));
        double maxScaling = Double.parseDouble(args.getOptionValue("max-scaling", DEFAULT_MAX_SCALING));
        double alpha = Double.parseDouble(args.getOptionValue("max-scaling", DEFAULT_ALPHA));
        boolean isStorageAware = Boolean.valueOf(args.getOptionValue("storage-aware", DEFAULT_IS_STORAGE_AWARE));

        VMFactory.readCliOptions(args, seed);

        CloudSimWrapper cloudsim = new CloudSimWrapper();
        cloudsim.init();
        cloudsim.setLogsEnabled(enableLogging);
        Log.disable(); // We do not need Cloudsim's logs. We have our own.

        // Determine the distribution
        String[] names = null;
        String inputname = inputdir.getAbsolutePath() + "/" + application;
        if ("uniform_unsorted".equals(distribution)) {
            names = DAGListGenerator.generateDAGListUniformUnsorted(new Random(seed), inputname, ensembleSize);
        } else if ("uniform_sorted".equals(distribution)) {
            names = DAGListGenerator.generateDAGListUniform(new Random(seed), inputname, ensembleSize);
        } else if ("pareto_unsorted".equals(distribution)) {
            names = DAGListGenerator.generateDAGListParetoUnsorted(new Random(seed), inputname, ensembleSize);
        } else if ("pareto_sorted".equals(distribution)) {
            names = DAGListGenerator.generateDAGListPareto(new Random(seed), inputname, ensembleSize);
        } else if ("constant".equals(distribution)) {
            names = DAGListGenerator.generateDAGListConstant(new Random(seed), inputname, ensembleSize);
        } else if (distribution.startsWith("fixed")) {
            int size = Integer.parseInt(distribution.substring(5));
            names = DAGListGenerator.generateDAGListConstant(inputname, size, ensembleSize);
        } else {
            System.err.println("Unrecognized distribution: " + distribution);
            System.exit(1);
        }

        StorageSimulationParams simulationParams = new StorageSimulationParams();

        if (storageCacheType.equals("fifo")) {
            simulationParams.setStorageCacheType(StorageCacheType.FIFO);
        } else if (storageCacheType.equals("void")) {
            simulationParams.setStorageCacheType(StorageCacheType.VOID);
        } else {
            throw new IllegalCWSArgumentException("Wrong storage-cache:" + storageCacheType);
        }

        if (storageManagerType.equals("global")) {
            GlobalStorageParams params = GlobalStorageParams.readCliOptions(args);
            simulationParams.setStorageParams(params);
            simulationParams.setStorageType(StorageType.GLOBAL);
        } else if (storageManagerType.equals("void")) {
            simulationParams.setStorageType(StorageType.VOID);
        } else {
            throw new IllegalCWSArgumentException("Wrong storage-manager:" + storageCacheType);
        }

        // Echo the simulation parameters
        System.out.printf("application = %s\n", application);
        System.out.printf("inputdir = %s\n", inputdir);
        System.out.printf("outputfile = %s\n", outputfile);
        System.out.printf("distribution = %s\n", distribution);
        System.out.printf("ensembleSize = %d\n", ensembleSize);
        System.out.printf("scalingFactor = %f\n", scalingFactor);
        System.out.printf("algorithm = %s\n", algorithmName);
        System.out.printf("seed = %d\n", seed);
        System.out.printf("storageManagerType = %s\n", storageManagerType);
        System.out.printf("storageCache = %s\n", storageCacheType);
        System.out.printf("enableLogging = %b\n", enableLogging);
        System.out.printf("nbudgets = %d\n", nbudgets);
        System.out.printf("ndeadlines = %d\n", ndeadlines);
        System.out.printf("alpha = %f\n", alpha);
        System.out.printf("maxScaling = %f\n", maxScaling);
        System.out.printf("isStorageAware = %b\n", isStorageAware);

        List<DAG> dags = new ArrayList<DAG>();
        Environment environment = EnvironmentFactory.createEnvironment(cloudsim, simulationParams,
                VMTypeBuilder.DEFAULT_VM_TYPE, isStorageAware);
        double minTime = Double.MAX_VALUE;
        double minCost = Double.MAX_VALUE;
        double maxCost = 0.0;
        double maxTime = 0.0;
        int workflow_id = 0;
        for (String name : names) {
            DAG dag = DAGParser.parseDAG(new File(name));
            dag.setId(new Integer(workflow_id).toString());
            System.out.println(String.format("Workflow %d, priority = %d, filename = %s", workflow_id, names.length
                    - workflow_id, name));
            workflow_id++;
            dags.add(dag);

            if (scalingFactor > 1.0) {
                for (String tid : dag.getTasks()) {
                    Task t = dag.getTaskById(tid);
                    t.scaleSize(scalingFactor);
                }
            }

            DAGStats dagStats = new DAGStats(dag, environment);

            minTime = Math.min(minTime, dagStats.getCriticalPath());
            minCost = Math.min(minCost, dagStats.getMinCost());

            maxTime += dagStats.getCriticalPath();
            maxCost += dagStats.getMinCost();
        }

        double minBudget;
        double maxBudget;
        double budgetStep = 0;
        if (args.getOptionValue("budget") == null) {
            minBudget = Math.ceil(minCost);
            maxBudget = Math.ceil(maxCost);
            budgetStep = (maxBudget - minBudget) / (nbudgets - 1);
        } else {
            minBudget = Double.valueOf(args.getOptionValue("budget"));
            maxBudget = minBudget;
        }
        if (budgetStep == 0) {
            budgetStep = 1;
        }

        double minDeadline;
        double maxDeadline;
        double deadlineStep = 0;
        if (args.getOptionValue("deadline") == null) {
            minDeadline = Math.ceil(minTime);
            maxDeadline = Math.ceil(maxTime);
            deadlineStep = (maxDeadline - minDeadline) / (ndeadlines - 1);
        } else {
            minDeadline = Double.valueOf(args.getOptionValue("deadline"));
            maxDeadline = minDeadline;
        }
        if (deadlineStep == 0) {
            deadlineStep = 1;
        }

        System.out.printf("budgets (min, max, step) = %f %f %f\n", minBudget, maxBudget, budgetStep);
        System.out.printf("deadlines (min, max, step) = %f %f %f\n", minDeadline, maxDeadline, deadlineStep);

        PrintStream fileOut = null;
        try {
            fileOut = new PrintStream(new FileOutputStream(outputfile));
            fileOut.println("application,distribution,seed,dags,scale,budget,"
                    + "deadline,algorithm,completed,exponential,linear,"
                    + "planning,simulation,scorebits,cost,jobfinish,dagfinish,"
                    + "vmfinish,runtimeVariance,delay,failureRate,minBudget," + "maxBudget,minDeadline,maxDeadline,"
                    + "storageManagerType,totalBytesToRead,totalBytesToWrite,totalBytesToTransfer,"
                    + "actualBytesRead,actualBytesTransferred,"
                    + "totalFilesToRead,totalFilesToWrite,totalFilesToTransfer,"
                    + "actualFilesRead,actualFilesTransferred");

            for (double budget = minBudget; budget <= maxBudget + (budgetStep / 2.0); budget += budgetStep) {
                System.out.println();
                for (double deadline = minDeadline; deadline <= maxDeadline + (deadlineStep / 2.0); deadline += deadlineStep) {
                    System.out.print(".");
                    if (enableLogging) {
                        cloudsim = new CloudSimWrapper(getLogOutputStream(budget, deadline, outputfile));
                    } else {
                        cloudsim = new CloudSimWrapper();
                    }
                    cloudsim.init();
                    cloudsim.setLogsEnabled(enableLogging);
                    cloudsim.log("budget = " + budget);
                    cloudsim.log("deadline = " + deadline);
                    logWorkflowsDescription(dags, names, cloudsim);

                    environment = EnvironmentFactory.createEnvironment(cloudsim, simulationParams,
                            VMTypeBuilder.DEFAULT_VM_TYPE, isStorageAware);

                    Algorithm algorithm = createAlgorithm(alpha, maxScaling, algorithmName, cloudsim, dags, budget,
                            deadline);

                    algorithm.setEnvironment(environment);
                    algorithm.simulate();

                    AlgorithmStatistics algorithmStatistics = algorithm.getAlgorithmStatistics();
                    double planningTime = algorithm.getPlanningnWallTime() / 1.0e9;
                    double simulationTime = cloudsim.getSimulationWallTime() / 1.0e9;

                    fileOut.printf("%s,%s,%d,%d,", application, distribution, seed, ensembleSize);
                    fileOut.printf("%f,%f,%f,%s,", scalingFactor, budget, deadline, algorithm.getName());
                    fileOut.printf("%d,%.10f,%.10f,%f,", algorithmStatistics.getFinishedDags().size(),
                            algorithmStatistics.getExponentialScore(), algorithmStatistics.getLinearScore(),
                            planningTime);
                    fileOut.printf("%f,%s,%f,%f,%f,", simulationTime, algorithmStatistics.getScoreBitString(),
                            algorithmStatistics.getActualCost(), algorithmStatistics.getActualJobFinishTime(),
                            algorithmStatistics.getActualDagFinishTime());
                    fileOut.printf("%f,%f,%f,%f,%f,%f,%f,%f,", algorithmStatistics.getActualVMFinishTime(),
                            VMFactory.getRuntimeVariance(), environment.getVMType().getProvisioningDelay(),
                            VMFactory.getFailureRate(), minBudget, maxBudget, minDeadline, maxDeadline);

                    StorageManagerStatistics stats = environment.getStorageManagerStatistics();
                    fileOut.printf("%s,%d,%d,%d,%d,%d,", storageManagerType, stats.getTotalBytesToRead(),
                            stats.getTotalBytesToWrite(), stats.getTotalBytesToRead() + stats.getTotalBytesToWrite(),
                            stats.getActualBytesRead(), stats.getActualBytesRead() + stats.getTotalBytesToWrite());

                    fileOut.printf("%d,%d,%d,%d,%d\n", stats.getTotalFilesToRead(), stats.getTotalFilesToWrite(),
                            stats.getTotalFilesToRead() + stats.getTotalFilesToWrite(), stats.getActualFilesRead(),
                            stats.getActualFilesRead() + stats.getTotalFilesToWrite());
                }
            }
            System.out.println();
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } finally {
            IOUtils.closeQuietly(fileOut);
        }
    }

    private void logWorkflowsDescription(List<DAG> dags, String[] names, CloudSimWrapper cloudsim) {
        for (int i = 0; i < dags.size(); i++) {
            DAG dag = dags.get(i);
            String workflowDescription = String.format("Workflow %s, priority = %d, filename = %s", dag.getId(),
                    dags.size() - i, names[i]);
            cloudsim.log(workflowDescription);
        }
    }

    /**
     * Crates algorithm instance from the given input params.
     * @return The newly created algorithm instance.
     */
    protected Algorithm createAlgorithm(double alpha, double maxScaling, String algorithmName,
            CloudSimWrapper cloudsim, List<DAG> dags, double budget, double deadline) {
        AlgorithmStatistics ensembleStatistics = new AlgorithmStatistics(dags, cloudsim);

        if ("SPSS".equals(algorithmName)) {
            return new SPSS(budget, deadline, dags, alpha, ensembleStatistics, cloudsim);
        } else if ("DPDS".equals(algorithmName)) {
            return new DPDS(budget, deadline, dags, maxScaling, ensembleStatistics, cloudsim);
        } else if ("WADPDS".equals(algorithmName)) {
            return new WADPDS(budget, deadline, dags, maxScaling, ensembleStatistics, cloudsim);
        } else {
            throw new IllegalCWSArgumentException("Unknown algorithm: " + algorithmName);
        }
    }

    /**
     * Returns output stream for logs for current simulation.
     * @param budget The simulation's budget.
     * @param deadline The simulation's deadline.
     * @param outputfile The simulation's main output file.
     * @return Output stream for logs for current simulation.
     */
    private OutputStream getLogOutputStream(double budget, double deadline, File outputfile)
            throws FileNotFoundException {
        String name = String.format("%s.b-%.2f-d-%.2f.log", outputfile.getAbsolutePath(), budget, deadline);
        return new FileOutputStream(new File(name));
    }
}
