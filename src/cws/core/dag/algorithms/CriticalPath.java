package cws.core.dag.algorithms;

import java.util.HashMap;
import java.util.Map;

import cws.core.dag.DAG;
import cws.core.dag.Task;


/**
 * Compute longest path using topological order, 
 * http://en.wikipedia.org/wiki/Longest_path_problem#Weighted_directed_acyclic_graphs
 * @author malawski
 */
public class CriticalPath {
    private Map<Task, Double> eft;
    private Double length = null;
    
    public CriticalPath(DAG dag, TopologicalOrder order) {
        eft = new HashMap<Task, Double>();

        // Initially the finish time is whatever the runtime is
        for (Task task : order) {
            eft.put(task, task.size);
        }
        
        // Now we adjust the values in the topological order
        for (Task task : order) {
            for (Task child : task.children) {
                eft.put(child, Math.max(eft.get(child), eft.get(task)+child.size));
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
     * @return Earliest start time of task
     */
    public double est(Task task) {
        return eft.get(task) - task.size;
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
