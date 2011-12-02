package cws.core.dag.algorithms;

import java.util.HashMap;
import java.util.Map;

import cws.core.dag.DAG;
import cws.core.dag.Task;


/**
 * Compute longest path using topological order, 
 * http://en.wikipedia.org/wiki/Longest_path_problem#Weighted_directed_acyclic_graphs
 * @author malawski
 *
 */
public class CriticalPath {
	
	// earliest start time
	Map<Task, Double> eft;
	
	public CriticalPath(DAG dag, TopologicalOrder order) {
		
		eft = new HashMap<Task, Double>();

		for (Task task : order.order()) {
			eft.put(task, task.size);
		}
		
		for (Task task : order.order()) {
			for (Task child : task.children) {
				eft.put(child, Math.max(eft.get(child), eft.get(task)+child.size));
			}
		}		
	}
	
	public double eft(Task task) {
		return eft.get(task);
	}

	public double est(Task task) {
		return eft.get(task) - task.size;
	}
	
	
	public double getCriticalPathLength() {
		double len = 0.0;
		
		for (Task task : eft.keySet()) {
			if (eft(task)>len) len=eft(task);
		}
		
		return len;
	}
	
}
