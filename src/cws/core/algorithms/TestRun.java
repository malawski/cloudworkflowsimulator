package cws.core.algorithms;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import cws.core.dag.DAG;
import cws.core.dag.DAGParser;
import cws.core.experiment.DAGListGenerator;

public class TestRun {
    public static void main(String[] args) throws Exception {
        List<DAG> dags = new ArrayList<DAG>();
        
        String[] names = DAGListGenerator.generateDAGListPareto(
                new Random(0), 
                "../projects/pegasus/CyberShake/CYBERSHAKE", 
                100);
        
        for (String name : names) {
            DAG dag = DAGParser.parseDAG(new File(name));
            
            // LARGER TASKS
            /*
            for (String tid : dag.getTasks()) {
                Task t = dag.getTask(tid);
                t.size = t.size * 100;
            }
            */
            dags.add(dag);
        }
        
        double deadline = 11*3600;
        double budget = 150;
        double price = 1;
        
        double alpha = 0.7;
        
        double maxScaling = 0.0;
        
        Algorithm[] algos = new Algorithm[]{
                /*
            
            new MaxMin(budget, deadline, dags),
            new MinMin(budget, deadline, dags),*/
            //new Wide(budget, deadline, dags),
            //new Backtrack(budget, deadline, dags),
            new SPSS(budget, deadline, dags, alpha),
            //new DPDS(budget, deadline, dags, price, maxScaling),
            new WADPDS(budget, deadline, dags, price, maxScaling)
        };
        
        for (Algorithm a : algos) {
            a.simulate(a.getName());
        }
        
        System.out.println("DAGs Completed:");
        for (Algorithm a : algos) {
            System.out.printf("    %10s: %s\n", a.getName(), a.completedDAGPriorityString());
        }
        
        System.out.println("Num DAGs Completed:");
        for (Algorithm a : algos) {
            System.out.printf("    %10s: %d\n", a.getName(), a.numCompletedDAGs());
        }
        
        System.out.println("Exponential score:");
        for (Algorithm a : algos) {
            System.out.printf("    %10s: %.10f\n", a.getName(), a.getExponentialScore());
        }
        
        System.out.println("Linear score:");
        for (Algorithm a : algos) {
            System.out.printf("    %10s: %.10f\n", a.getName(), a.getLinearScore());
        }
    }
}
