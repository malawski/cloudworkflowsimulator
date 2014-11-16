package cws.core.dag.algorithms;

import java.util.HashMap;
import java.util.Map;

import cws.core.dag.Task;
import cws.core.engine.Environment;

/**
 * Compute longest path using topological order,
 * http://en.wikipedia.org/wiki/Longest_path_problem#Weighted_directed_acyclic_graphs
 * @author malawski
 */
public class CriticalPath {
    private final Map<Task, Double> eft = new HashMap<Task, Double>();

    public CriticalPath(TopologicalOrder order, Environment environment) {
        this(order, null, environment);
    }

    public CriticalPath(TopologicalOrder order, Map<Task, Double> runtimes, Environment environment) {
        if (runtimes == null) {
            runtimes = new HashMap<Task, Double>();
            for (Task task : order) {
                runtimes.put(task, getPredictedTaskRuntime(environment, task));
            }
        }

        // Initially the finish time is whatever the runtime is
        for (Task task : order) {
            eft.put(task, runtimes.get(task));
        }

        // Now we adjust the values in the topological order
        for (Task task : order) {
            for (Task child : task.getChildren()) {
                eft.put(child, Math.max(eft.get(child), eft.get(task) + runtimes.get(child)));
            }
        }
    }

    /**
     * Estimates and returns predicted task's runtime. May be overridden by subclasses to return estimations based on
     * different criteria.
     */
    protected double getPredictedTaskRuntime(Environment environment, Task task) {
        return environment.getComputationPredictedRuntime(task);
    }

    /**
     * @return Earliest finish time of task
     */
    public double getEarliestFinishTime(Task task) {
        return eft.get(task);
    }

    /**
     * @return Length of critical path
     */
    public double getCriticalPathLength() {
        double len = 0.0;
        for (Task task : eft.keySet()) {
            double eft = getEarliestFinishTime(task);
            if (eft > len)
                len = eft;
        }
        return len;
    }
}
