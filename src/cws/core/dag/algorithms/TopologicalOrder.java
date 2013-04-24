package cws.core.dag.algorithms;

import java.util.Deque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Set;

import cws.core.dag.ComputationTask;
import cws.core.dag.DAG;

/**
 * Compute topological order of a DAG.
 * Uses depth-first search.
 * A reverse postorder in a DAG provides a topological order.
 * Reverse postorder: Put the vertex on a stack after the recursive calls.
 * See: http://algs4.cs.princeton.edu/42directed/
 * 
 * @author malawski
 * 
 */
public class TopologicalOrder implements Iterable<ComputationTask> {
    private Set<ComputationTask> marked = new HashSet<ComputationTask>();
    private Deque<ComputationTask> postorder = new LinkedList<ComputationTask>();

    public TopologicalOrder(DAG dag) {
        for (String taskName : dag.getTasks()) {
            ComputationTask task = dag.getTaskById(taskName);
            if (!marked.contains(task))
                dfs(task);
        }
        marked = null;
    }

    private void dfs(ComputationTask task) {
        marked.add(task);
        for (ComputationTask child : task.getChildren()) {
            if (!marked.contains(child))
                dfs(child);
        }
        postorder.add(task);
    }

    public Iterable<ComputationTask> reverse() {
        return new Iterable<ComputationTask>() {
            @Override
            public Iterator<ComputationTask> iterator() {
                return postorder.iterator();
            }
        };
    }

    @Override
    public Iterator<ComputationTask> iterator() {
        return postorder.descendingIterator();
    }
}
