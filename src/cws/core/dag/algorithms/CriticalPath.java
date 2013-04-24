package cws.core.dag.algorithms;

import java.util.HashMap;
import java.util.Map;

import cws.core.dag.ComputationTask;
import cws.core.dag.Task;

/**
 * Compute longest path using topological order,
 * http://en.wikipedia.org/wiki/Longest_path_problem#Weighted_directed_acyclic_graphs
 * @author malawski
 */
public class CriticalPath {
    private Map<ComputationTask, Double> eft;
    private Double length = null;

    public CriticalPath(TopologicalOrder order) {
        this(order, null);
    }

    public CriticalPath(TopologicalOrder order, Map<ComputationTask, Double> runtimes) {
        this.eft = new HashMap<ComputationTask, Double>();

        /*
         * XXX By default use the task size as its runtime. This is not strictly
         * correct because the size is in MI and the runtime depends on the VM
         * type that the task runs on.
         */
        if (runtimes == null) {
            runtimes = new HashMap<ComputationTask, Double>();
            for (ComputationTask task : order) {
                runtimes.put(task, task.getSize());
            }
        }

        // Initially the finish time is whatever the runtime is
        for (ComputationTask task : order) {
            eft.put(task, runtimes.get(task));
        }

        // Now we adjust the values in the topological order
        for (ComputationTask task : order) {
            for (ComputationTask child : task.getChildren()) {
                eft.put(child, Math.max(eft.get(child), eft.get(task) + runtimes.get(child)));
            }
        }
    }

    /**
     * @return Earliest finish time of task
     */
    public double eft(Task task) {
        return eft.get(task);
    }

    /**
     * @return Length of critical path
     */
    public double getCriticalPathLength() {
        if (length == null) { // Cache
            double len = 0.0;
            for (Task task : eft.keySet()) {
                double eft = eft(task);
                if (eft > len)
                    len = eft;
            }
            length = len;
        }
        return length;
    }
}
