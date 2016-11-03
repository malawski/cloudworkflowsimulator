package cws.core.dag;

import java.util.HashMap;

import cws.core.core.VMType;
import cws.core.dag.algorithms.CriticalPath;
import cws.core.dag.algorithms.TopologicalOrder;
import cws.core.engine.Environment;

public class DAGStats {
    private double minCost;
    private double criticalPathLength;
    private double totalRuntime;
    private Environment environment;

    public DAGStats(DAG dag, VMType vmType, Environment environment) {
        this.environment = environment;
        TopologicalOrder order = new TopologicalOrder(dag);

        HashMap<Task, Double> runTimes = computeMinimumCostOfRunningTheWorkflow(order, vmType);

        // Make sure a plan is feasible given the deadline and available VMs
        CriticalPath path = new CriticalPath(order, runTimes, vmType);
        criticalPathLength = path.getCriticalPathLength();
    }

    private HashMap<Task, Double> computeMinimumCostOfRunningTheWorkflow(TopologicalOrder order, VMType vmType) {
        totalRuntime = 0.0;
        HashMap<Task, Double> runTimes = new HashMap<Task, Double>();
        for (Task task : order) {
            double runtime = vmType.getPredictedTaskRuntime(task);
            runTimes.put(task, runtime);
            totalRuntime += runtime;
        }

        minCost = environment.getPricingManager().getVMCostFor(vmType, totalRuntime);
        return runTimes;
    }

    public double getMinCost() {
        return minCost;
    }

    public double getCriticalPathLength() {
        return criticalPathLength;
    }

    public double getTotalRuntime() {
        return totalRuntime;
    }
}
