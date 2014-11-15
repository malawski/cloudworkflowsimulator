package cws.core.dag;

import java.util.HashMap;

import cws.core.dag.algorithms.CriticalPath;
import cws.core.dag.algorithms.TopologicalOrder;
import cws.core.engine.Environment;

public class DAGStats {
    private double minCost;
    private double criticalPath;
    private double totalRuntime;

    public DAGStats(DAG dag, Environment environment) {
        TopologicalOrder order = new TopologicalOrder(dag);

        HashMap<Task, Double> runTimes = computeMinimumCostOfRunningTheWorkflow(environment, order);

        // Make sure a plan is feasible given the deadline and available VMs
        CriticalPath path = new CriticalPath(order, runTimes, environment);
        criticalPath = path.getCriticalPathLength();
    }

    private HashMap<Task, Double> computeMinimumCostOfRunningTheWorkflow(Environment environment, TopologicalOrder order) {
        totalRuntime = 0.0;
        HashMap<Task, Double> runTimes = new HashMap<Task, Double>();
        for (Task task : order) {
            double runtime = environment.getComputationPredictedRuntime(task);
            runTimes.put(task, runtime);
            totalRuntime += runtime;
        }

        minCost = environment.getVMCostFor(totalRuntime);
        return runTimes;
    }

    public double getMinCost() {
        return minCost;
    }

    public double getCriticalPath() {
        return criticalPath;
    }

    public double getTotalRuntime() {
        return totalRuntime;
    }
}
