package cws.core.algorithms;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
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

import cws.core.AlgorithmStatistics;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.dag.DAG;
import cws.core.dag.DAGListGenerator;
import cws.core.dag.DAGParser;
import cws.core.dag.DAGStats;
import cws.core.dag.Task;
import cws.core.exception.IllegalCWSArgumentException;
import cws.core.provisioner.VMFactory;
import cws.core.storage.StorageManagerStatistics;
import cws.core.storage.global.GlobalStorageParams;

public class TestRun {
    private static final String DEFAULT_ENSEMBLE_SIZE = "50";
    private static final String DEFAULT_SCALING_FACTOR = "1.0";
    private static final String DEFAULT_STORAGE_CACHE = "void";
    private static final String DEFAULT_ENABLE_LOGGING = "true";

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
        enableLogging.setArgName("DEADLINE");
        options.addOption(deadline);

        Option budget = new Option("b", "budget", true, "Optional budget, which overrides max and min budgets");
        enableLogging.setArgName("DEADLINE");
        options.addOption(budget);

        GlobalStorageParams.buildCliOptions(options);
        VMFactory.buildCliOptions(options);
        return options;
    }

    private static void printUsage(Options options, String reason) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.setWidth(120);
        formatter.printHelp(TestRun.class.getName(), "", options, reason);
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
        TestRun testRun = new TestRun();
        try {
            testRun.runTest(cmd);
        } catch (IllegalCWSArgumentException e) {
            printUsage(options, e.getMessage());
        }
    }

    public void runTest(CommandLine args) {
        // These parameters are consistent with previous experiments
        double alpha = 0.7;
        double maxScaling = 1.0;

        // Arguments with no defaults
        String algorithmName = args.getOptionValue("algorithm"); // "SPSS";
        String application = args.getOptionValue("application"); // "SIPHT";
        File inputdir = new File(args.getOptionValue("input-dir"));
        File outputfile = new File(args.getOptionValue("output-file")); // new File("TestRun.dat");
        String distribution = args.getOptionValue("distribution"); // "uniform_unsorted";
        String storageManagerType = args.getOptionValue("storage-manager");

        // Arguments with defaults
        Integer ensembleSize = Integer.parseInt(args.getOptionValue("ensemble-size", DEFAULT_ENSEMBLE_SIZE));
        Double scalingFactor = Double.parseDouble(args.getOptionValue("scaling-factor", DEFAULT_SCALING_FACTOR));
        Long seed = Long.parseLong(args.getOptionValue("seed", System.currentTimeMillis() + ""));
        String storageCacheType = args.getOptionValue("storage-cache", DEFAULT_STORAGE_CACHE);
        Boolean enableLogging = Boolean.valueOf(args.getOptionValue("enable-logging", DEFAULT_ENABLE_LOGGING));

        VMFactory.readCliOptions(args, seed);

        CloudSimWrapper cloudsim = new CloudSimWrapper();
        cloudsim.init();

        // Disable cloudsim logging
        if (!enableLogging) {
            cloudsim.disableLogging();
        }

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

        double minTime = Double.MAX_VALUE;
        double minCost = Double.MAX_VALUE;
        double maxCost = 0.0;
        double maxTime = 0.0;

        List<DAG> dags = new ArrayList<DAG>();
        int workflow_id = 0;
        for (String name : names) {
            DAG dag = DAGParser.parseDAG(new File(name));
            dag.setId(new Integer(workflow_id).toString());
            System.out.println(String.format("Workflow %d, priority = %d, filename = %s", workflow_id, workflow_id,
                    name));
            workflow_id++;
            dags.add(dag);

            if (scalingFactor > 1.0) {
                for (String tid : dag.getTasks()) {
                    Task t = dag.getTaskById(tid);
                    t.scaleSize(scalingFactor);
                }
            }

            DAGStats dagStats = new DAGStats(dag, Algorithm.initializeStorage(simulationParams, cloudsim));

            minTime = Math.min(minTime, dagStats.getCriticalPath());
            minCost = Math.min(minCost, dagStats.getMinCost());

            maxTime += dagStats.getCriticalPath();
            maxCost += dagStats.getMinCost();
        }

        int nbudgets = 10;
        int ndeadlines = 10;

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

        System.out.printf("budget = %f %f %f\n", minBudget, maxBudget, budgetStep);
        System.out.printf("deadline = %f %f %f\n", minDeadline, maxDeadline, deadlineStep);

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
                    cloudsim = new CloudSimWrapper();
                    cloudsim.init();
                    Algorithm algorithm = createAlgorithm(alpha, maxScaling, algorithmName, cloudsim, simulationParams,
                            dags, budget, deadline);

                    algorithm.simulate(algorithmName);

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
                            VMFactory.getRuntimeVariance(), VMFactory.getProvisioningDelay(),
                            VMFactory.getFailureRate(), minBudget, maxBudget, minDeadline, maxDeadline);

                    StorageManagerStatistics stats = algorithm.getStorageManager().getStorageManagerStatistics();
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

    /**
     * Crates algorithm instance from the given input params.
     * @return The newly created algorithm instance.
     */
    protected Algorithm createAlgorithm(double alpha, double maxScaling, String algorithmName,
            CloudSimWrapper cloudsim, StorageSimulationParams simulationParams, List<DAG> dags, double budget,
            double deadline) {
        AlgorithmStatistics ensembleStatistics = new AlgorithmStatistics(dags, cloudsim);
        if ("SPSS".equals(algorithmName)) {
            return new SPSS(budget, deadline, dags, alpha, simulationParams, ensembleStatistics, cloudsim);
        } else if ("DPDS".equals(algorithmName)) {
            return new DPDS(budget, deadline, dags, VMType.DEFAULT_VM_TYPE.getPrice(), maxScaling, simulationParams,
                    ensembleStatistics, cloudsim);
        } else if ("WADPDS".equals(algorithmName)) {
            return new WADPDS(budget, deadline, dags, VMType.DEFAULT_VM_TYPE.getPrice(), maxScaling, simulationParams,
                    ensembleStatistics, cloudsim);
        } else {
            throw new IllegalCWSArgumentException("Unknown algorithm: " + algorithmName);
        }
    }
}
