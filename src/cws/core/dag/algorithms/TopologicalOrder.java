package cws.core.dag.algorithms;

import java.util.Deque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Set;

import cws.core.dag.DAG;
import cws.core.dag.Task;

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
public class TopologicalOrder implements Iterable<Task> {
    private Set<Task> marked = new HashSet<Task>();
    private Deque<Task> postorder = new LinkedList<Task>();

    public TopologicalOrder(DAG dag) {
        for (String taskName : dag.getTasks()) {
            Task task = dag.getTaskById(taskName);
            if (!marked.contains(task))
                dfs(task);
        }
        marked = null;
    }

    private void dfs(Task task) {
        marked.add(task);
        for (Task child : task.getChildren()) {
            if (!marked.contains(child))
                dfs(child);
        }
        postorder.add(task);
    }

    public Iterable<Task> reverse() {
        return new Iterable<Task>() {
            @Override
            public Iterator<Task> iterator() {
                return postorder.iterator();
            }
        };
    }

    @Override
    public Iterator<Task> iterator() {
        return postorder.descendingIterator();
    }
}
