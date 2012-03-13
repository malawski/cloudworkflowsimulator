package cws.core.algorithms;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

import org.cloudbus.cloudsim.Log;

import cws.core.UniformRuntimeDistribution;
import cws.core.dag.DAG;
import cws.core.dag.Task;
import cws.core.dag.DAGParser;
import cws.core.dag.algorithms.CriticalPath;
import cws.core.dag.algorithms.TopologicalOrder;
import cws.core.experiment.DAGListGenerator;

public class TestRun {
    
    static class DAGStats {
        public double minCost;
        public double criticalPath;
        public double totalRuntime;
        
        public DAGStats(DAG dag, double mips, double price) {
            TopologicalOrder order = new TopologicalOrder(dag);
            
            minCost = 0.0;
            totalRuntime = 0.0;
            
            HashMap<Task, Double> runtimes = new HashMap<Task, Double>();
            for (Task t : order) {
                
                // The runtime is just the size of the task (MI) divided by the
                // MIPS of the VM
                double runtime = t.size / mips;
                runtimes.put(t, runtime);
                
                // Compute the minimum cost of running this workflow
                minCost += (runtime/(60*60)) * price;
                totalRuntime += runtime;
            }
            
            // Make sure a plan is feasible given the deadline and available VMs
            CriticalPath path = new CriticalPath(order, runtimes);
            criticalPath = path.getCriticalPathLength();
        }
    }
    
    public static void usage() {
        System.err.printf("Usage: %s application inputdir outputfile distribution ensembleSize scalingFactor algorithm seed runtimeVariance\n\n", TestRun.class.getName());
        System.exit(1);
    }
    
    public static void main(String[] args) throws Exception {
        // These parameters are consistent with previous experiments
        double alpha = 0.7;
        double maxScaling = 1.0;
        
        // WARNING: These parameters are fixed in the algorithm! Don't change here only!
        double mips = 1;
        double price = 1;
        
        if (args.length != 9) {
            usage();
        }
        
        /*
        String application = "SIPHT";
        File inputdir = new File("/Volumes/HDD/SyntheticWorkflows/SIPHT");
        File outputdir = new File("/tmp");
        String distribution = "uniform_unsorted";
        int ensembleSize = 50;
        double scalingFactor = 1.0;
        String algorithm = "SPSS";
        int seed = 0;
        double runtimeVariance = 0.0;
        */
        
        // Disable cloudsim logging
        Log.disable();
        
        String application = args[0];
        File inputdir = new File(args[1]);
        File outputfile = new File(args[2]);
        String distribution = args[3];
        int ensembleSize = Integer.parseInt(args[4]);
        double scalingFactor = Double.parseDouble(args[5]);
        String algorithm = args[6];
        int seed = Integer.parseInt(args[7]);
        double runtimeVariance = Double.parseDouble(args[8]);
        
        System.out.printf("application = %s\n", application);
        System.out.printf("inputdir = %s\n", inputdir);
        System.out.printf("outputfile = %s\n", outputfile);
        System.out.printf("distribution = %s\n", distribution);
        System.out.printf("ensembleSize = %d\n", ensembleSize);
        System.out.printf("scalingFactor = %f\n", scalingFactor);
        System.out.printf("algorithm = %s\n", algorithm);
        System.out.printf("seed = %d\n", seed);
        System.out.printf("runtimeVariance = %f\n", runtimeVariance);
        
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
            
            minTime = Math.min(minTime, s.totalRuntime);
            minCost = Math.min(minCost, s.minCost);
            
            maxTime += s.criticalPath;
            maxCost += s.minCost;
        }
        
        // Add 10 percent
        minTime *= 1.1;
        minCost *= 1.1;
        
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
        
        out.println("application,distribution,seed,dags,scale,budget,deadline,algorithm,completed,exponential,linear,planning,simulation,scorebits,cost,makespan,runtimeVariance");
        
        for (double budget = minBudget; budget <= maxBudget; budget += budgetStep) {
            for (double deadline = minDeadline; deadline <= maxDeadline; deadline+= deadlineStep) {
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
                    a.getVmFactory().setRuntimeDistribution(
                            new UniformRuntimeDistribution(seed, runtimeVariance));
                }
                
                a.simulate(algorithm);
                
                double planningTime = a.getPlanningnWallTime() / 1.0e9;
                double simulationTime = a.getSimulationWallTime() / 1.0e9;
                
                out.printf("%s,%s,%d,%d,%f,%f,%f,%s,%d,%.20f,%.20f,%f,%f,%s,%f,%f,%f\n", 
                        application, distribution, seed, ensembleSize, scalingFactor, budget, deadline, 
                        a.getName(), a.numCompletedDAGs(), a.getExponentialScore(), a.getLinearScore(),
                        planningTime, simulationTime, a.getScoreBitString(), a.getActualCost(), 
                        a.getActualFinishTime(), runtimeVariance);
            }
        }
        
        out.close();
    }
}
