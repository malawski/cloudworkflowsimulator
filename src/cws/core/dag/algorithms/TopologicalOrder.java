package cws.core.dag.algorithms;

import java.util.Deque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;

import cws.core.dag.DAG;
import cws.core.dag.Task;


/** 
 * 
 * Compute topological order of a DAG.
 * Uses depth-first search.
 * A reverse postorder in a DAG provides a topological order. 
 * Reverse postorder: Put the vertex on a stack after the recursive calls. 
 * See: http://algs4.cs.princeton.edu/42directed/
 * 
 * @author malawski
 *
 */


public class TopologicalOrder {
	
	private DAG dag;
	private Set<Task> marked = new HashSet<Task>();
	private Deque<Task> postorder = new LinkedList<Task>();
	
	public TopologicalOrder(DAG dag) {
		this.dag = dag;
		
		for (String taskName : dag.getTasks()) {
			Task task = dag.getTask(taskName);
			if (!marked.contains(task)) dfs (task);
		}
	}

	private void dfs(Task task) {
		marked.add(task);
		for (Task child : task.children) {
			if (!marked.contains(child)) dfs (child);
		}
		postorder.add(task);
	}
	
	
    // return Tasks in postorder as an Iterable
    public Iterable<Task> postorder() {
        return postorder;
    }
    
    public Iterable<Task> reversePostorder() {
    	return new Iterable<Task>() {			
			@Override
			public Iterator<Task> iterator() {
				// TODO Auto-generated method stub
				return postorder.descendingIterator();
			}
		};
    	
    }

    // topological order is the reverse postorder
    public Iterable<Task> order() {
    	return reversePostorder();
    }
	
	
	
}
