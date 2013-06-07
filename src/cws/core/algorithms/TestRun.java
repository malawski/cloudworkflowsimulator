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

import cws.core.cloudsim.CloudSimWrapper;
import cws.core.dag.DAG;
import cws.core.dag.DAGParser;
import cws.core.dag.DAGStats;
import cws.core.dag.Task;
import cws.core.exception.WrongCommandLineArgsException;
import cws.core.experiment.DAGListGenerator;
import cws.core.experiment.VMFactory;
import cws.core.storage.StorageManager;
import cws.core.storage.StorageManagerStatistics;
import cws.core.storage.VoidStorageManager;
import cws.core.storage.cache.FIFOCacheManager;
import cws.core.storage.cache.VMCacheManager;
import cws.core.storage.cache.VoidCacheManager;
import cws.core.storage.global.GlobalStorageManager;
import cws.core.storage.global.GlobalStorageParams;

public class TestRun {
    private static final String DEFAULT_ENSEMBLE_SIZE = "50";
    private static final String DEFAULT_SCALING_FACTOR = "1.0";
    private static final String DEFAULT_STORAGE_CACHE = "void";

    private static Options buildOptions() {
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
        } catch (WrongCommandLineArgsException e) {
            printUsage(options, e.getMessage());
        }
    }

    public void runTest(CommandLine args) {
        // These parameters are consistent with previous experiments
        double alpha = 0.7;
        double maxScaling = 1.0;

        // WARNING: These parameters are fixed in the algorithm! Don't change here only!
        double mips = 1;
        double price = 1;

        // Arguments with no defaults
        String algorithm = args.getOptionValue("algorithm"); // "SPSS";
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

        VMFactory.readCliOptions(args, seed);

        // TODO(_mequrel_): change to IoC in the future
        CloudSimWrapper cloudsim = new CloudSimWrapper();
        cloudsim.init();

        // Disable cloudsim logging
        cloudsim.disableLogging();

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

        StorageManager storageManager = null;

        VMCacheManager cacheManager = null;
        if (storageCacheType.equals("fifo")) {
            cacheManager = new FIFOCacheManager(cloudsim);
        } else if (storageCacheType.equals("void")) {
            cacheManager = new VoidCacheManager(cloudsim);
        } else {
            throw new WrongCommandLineArgsException("Wrong storage-cache:" + storageCacheType);
        }

        if (storageManagerType.equals("global")) {
            GlobalStorageParams params = GlobalStorageParams.readCliOptions(args);
            storageManager = new GlobalStorageManager(params, cacheManager, cloudsim);
        } else {
            storageManager = new VoidStorageManager(cloudsim);
        }

        // Echo the simulation parameters
        System.out.printf("application = %s\n", application);
        System.out.printf("inputdir = %s\n", inputdir);
        System.out.printf("outputfile = %s\n", outputfile);
        System.out.printf("distribution = %s\n", distribution);
        System.out.printf("ensembleSize = %d\n", ensembleSize);
        System.out.printf("scalingFactor = %f\n", scalingFactor);
        System.out.printf("algorithm = %s\n", algorithm);
        System.out.printf("seed = %d\n", seed);
        // TODO(bryk): @mequrel: should storageManagerType be here? I believe so.
        System.out.printf("storageCache = %s\n", storageCacheType);

        double minTime = Double.MAX_VALUE;
        double minCost = Double.MAX_VALUE;
        double maxCost = 0.0;
        double maxTime = 0.0;

        List<DAG> dags = new ArrayList<DAG>();
        for (String name : names) {
            System.out.println(name);
            DAG dag = DAGParser.parseDAG(new File(name));
            dags.add(dag);

            if (scalingFactor > 1.0) {
                for (String tid : dag.getTasks()) {
                    Task t = dag.getTaskById(tid);
                    t.setSize(t.getSize() * scalingFactor);
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
                    + "actualBytesRead,actualBytesWritten,totalBytesTransferred");

            for (double budget = minBudget; budget < maxBudget + (budgetStep / 2.0); budget += budgetStep) {
                System.out.println();
                for (double deadline = minDeadline; deadline < maxDeadline + (deadlineStep / 2.0); deadline += deadlineStep) {
                    System.out.print(".");
                    Algorithm a = null;
                    if ("SPSS".equals(algorithm)) {
                        a = new SPSS(budget, deadline, dags, alpha, cloudsim, storageManager);
                    } else if ("DPDS".equals(algorithm)) {
                        a = new DPDS(budget, deadline, dags, price, maxScaling, cloudsim, storageManager);
                    } else if ("WADPDS".equals(algorithm)) {
                        a = new WADPDS(budget, deadline, dags, price, maxScaling, cloudsim, storageManager);
                    } else {
                        throw new WrongCommandLineArgsException("Unknown algorithm: " + algorithm);
                    }

                    a.simulate(algorithm);

                    double planningTime = a.getPlanningnWallTime() / 1.0e9;
                    double simulationTime = a.getSimulationWallTime() / 1.0e9;

                    fileOut.printf("%s,%s,%d,%d,", application, distribution, seed, ensembleSize);
                    fileOut.printf("%f,%f,%f,%s,", scalingFactor, budget, deadline, a.getName());
                    fileOut.printf("%d,%.20f,%.20f,%f,", a.numCompletedDAGs(), a.getExponentialScore(),
                            a.getLinearScore(), planningTime);
                    fileOut.printf("%f,%s,%f,%f,%f,", simulationTime, a.getScoreBitString(), a.getActualCost(),
                            a.getActualJobFinishTime(), a.getActualDagFinishTime());
                    fileOut.printf("%f,%f,%f,%f,%f,%f,%f,%f,", a.getActualVMFinishTime(),
                            VMFactory.getRuntimeVariance(), VMFactory.getDelay(), VMFactory.getFailureRate(),
                            minBudget, maxBudget, minDeadline, maxDeadline);

                    StorageManagerStatistics stats = a.getStorageManager().getStorageManagerStatistics();
                    fileOut.printf("%s,%d,%d,%d,%d,%d,%d\n", storageManagerType, stats.getTotalBytesToRead(),
                            stats.getTotalBytesToWrite(), stats.getTotalBytesToRead() + stats.getTotalBytesToWrite(),
                            stats.getActualBytesRead(), stats.getAcutalBytesWritten(), stats.getActualBytesRead()
                                    + stats.getAcutalBytesWritten());
                }
            }

        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } finally {
            IOUtils.closeQuietly(fileOut);
        }
    }
}
