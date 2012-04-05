package cws.core.algorithms;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.distributions.ContinuousDistribution;

import cws.core.FailureModel;
import cws.core.UniformRuntimeDistribution;
import cws.core.dag.DAG;
import cws.core.dag.DAGStats;
import cws.core.dag.Task;
import cws.core.dag.DAGParser;
import cws.core.experiment.DAGListGenerator;
import cws.core.experiment.VMFactory;
import java.lang.Math;

public class TestRun {
    
    static class ConstantDistribution implements ContinuousDistribution {
        private double delay;
        
        public ConstantDistribution(double delay) {
            this.delay = delay;
        }
        
        @Override
        public double sample() {
            return this.delay;
        }
    }
    
    public static void usage() {
        System.err.printf("Usage: %s -application APP -inputdir DIR \n\t-outputfile FILE -distribution DIST -ensembleSize SIZE \n\t-algorithm ALGO [-scalingFactor FACTOR] [-seed SEED] \n\t[-runtimeVariance VAR] [-delay DELAY] [-failureRate RATE]\n\n", TestRun.class.getName());
        System.exit(1);
    }
    
    public static void main(String[] args) throws Exception {
        // These parameters are consistent with previous experiments
        double alpha = 0.7;
        double maxScaling = 1.0;
        
        // WARNING: These parameters are fixed in the algorithm! Don't change here only!
        double mips = 1;
        double price = 1;
        
        // Arguments with no defaults
        String algorithm = null; //"SPSS";
        String application = null; //"SIPHT";
        File inputdir = null; //new File("/Volumes/HDD/SyntheticWorkflows/SIPHT");
        File outputfile = null; //new File("TestRun.dat");
        String distribution = null; //"uniform_unsorted";
        
        // Arguments with defaults
        Integer ensembleSize = 50;
        Double scalingFactor = 1.0;
        Long seed = System.currentTimeMillis();
        Double runtimeVariance = 0.0;
        Double delay = 0.0;
        Double failureRate = 0.0;
        
        try {
            for (int i = 0; i<args.length; ) {
                String arg = args[i++];
                String next = args[i++];
                
                if ("-application".equals(arg)) {
                    application = next;
                } else if ("-inputdir".equals(arg)) {
                    inputdir = new File(next);
                    if (!inputdir.isDirectory()) {
                        System.err.println("-inputdir not found");
                        System.exit(1);
                    }
                } else if ("-outputfile".equals(arg)) {
                    outputfile = new File(next);
                } else if ("-distribution".equals(arg)) {
                    distribution = next;
                } else if ("-ensembleSize".equals(arg)) {
                    ensembleSize = Integer.parseInt(next);
                } else if ("-scalingFactor".equals(arg)) {
                    scalingFactor = Double.parseDouble(next); 
                } else if ("-algorithm".equals(arg)) {
                    algorithm = next;
                } else if ("-seed".equals(arg)) {
                    seed = Long.parseLong(next);
                } else if ("-runtimeVariance".equals(arg)) {
                    runtimeVariance = Double.parseDouble(next);
                } else if ("-delay".equals(arg)) {
                    delay = Double.parseDouble(next);
                } else if ("-failureRate".equals(arg)) {
                    failureRate = Double.parseDouble(next);
                } else {
                    System.err.println("Illegal argument "+arg);
                    usage();
                }
            }
        } catch (ArrayIndexOutOfBoundsException e) {
            System.err.println("Invalid argument");
            usage();
        }
        
        if (application == null) {
            System.err.println("-application required");
            usage();
        }
        
        if (inputdir == null) {
            System.err.println("-inputdir required");
            usage();
        }
        
        if (outputfile == null) {
            System.err.println("-outputfile required");
            usage();
        }
        
        if (distribution == null) {
            System.err.println("-distribution required");
            usage();
        }
        
        if (ensembleSize == null) {
            System.err.println("-ensembleSize required");
            usage();
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
        System.out.printf("runtimeVariance = %f\n", runtimeVariance);
        System.out.printf("delay = %f\n", delay);
        System.out.printf("failureRate = %f\n", failureRate);
        
        // Disable cloudsim logging
        Log.disable();
        
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
            
        } else if ("constant".equals(distribution)){
            
            names = DAGListGenerator.generateDAGListConstant(new Random(seed), inputname, ensembleSize);
            
        } else if (distribution.startsWith("fixed")) {
            
            int size = Integer.parseInt(distribution.substring(5));
            names = DAGListGenerator.generateDAGListConstant(inputname, size, ensembleSize);
            
        } else {
            System.err.println("Unrecognized distribution: "+distribution);
            System.exit(1);
        }
        
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
                    Task t = dag.getTask(tid);
                    t.size *= scalingFactor;
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
        
        PrintStream out = new PrintStream(new FileOutputStream(outputfile));
        
        out.println("application,distribution,seed,dags,scale,budget,deadline,algorithm,completed,exponential,linear,planning,simulation,scorebits,cost,makespan,runtimeVariance,delay,failureRate,minBudget,maxBudget,minDeadline,maxDeadline");
        
        for (double budget = minBudget; budget < maxBudget+(budgetStep/2.0); budget += budgetStep) {
            System.out.println();
            for (double deadline = minDeadline; deadline < maxDeadline+(deadlineStep/2.0); deadline+= deadlineStep) {
                System.out.print(".");
                Algorithm a = null;
                if ("SPSS".equals(algorithm)) {
                    a = new SPSS(budget, deadline, dags, alpha);
                } else if ("DPDS".equals(algorithm)) {
                    a = new DPDS(budget, deadline, dags, price, maxScaling);
                } else if ("WADPDS".equals(algorithm)) {
                    a = new WADPDS(budget, deadline, dags, price, maxScaling);
                } else {
                    throw new RuntimeException("Unknown algorithm: "+algorithm);
                }
                
                if (runtimeVariance > 0.0) {
                    VMFactory.setRuntimeDistribution(
                            new UniformRuntimeDistribution(seed, runtimeVariance));
                }
                
                if (delay > 0.0) {
                    VMFactory.setProvisioningDelayDistribution(new ConstantDistribution(delay));
                }
                
                if (failureRate > 0.0) {
                    VMFactory.setFailureModel(new FailureModel(seed, failureRate));
                }
                
                a.simulate(algorithm);
                
                double planningTime = a.getPlanningnWallTime() / 1.0e9;
                double simulationTime = a.getSimulationWallTime() / 1.0e9;
                
                out.printf("%s,%s,%d,%d,%f,%f,%f,%s,%d,%.20f,%.20f,%f,%f,%s,%f,%f,%f,%f,%f\n", 
                        application, distribution, seed, ensembleSize, scalingFactor, budget, deadline, 
                        a.getName(), a.numCompletedDAGs(), a.getExponentialScore(), a.getLinearScore(),
                        planningTime, simulationTime, a.getScoreBitString(), a.getActualCost(), 
                        a.getActualFinishTime(), runtimeVariance, delay, failureRate, minBudget, maxBudget,
                        minDeadline, maxDeadline);
            }
        }
        
        out.close();
    }
}
