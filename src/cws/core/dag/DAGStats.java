package cws.core.dag;

import java.util.HashMap;

import cws.core.dag.algorithms.CriticalPath;
import cws.core.dag.algorithms.TopologicalOrder;

public class DAGStats {
    private double minCost;
    private double criticalPath;
    private double totalRuntime;
    
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
