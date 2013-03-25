package cws.core.dag.algorithms;

import java.io.File;
import java.util.ArrayList;

import org.junit.Test;
import static org.junit.Assert.*;

import cws.core.dag.DAG;
import cws.core.dag.DAGParser;
import cws.core.dag.Task;

public class TopologicalOrderTest {

    @Test
    public void topotest() {
        DAG dag = DAGParser.parseDAG(new File("dags/topotest.dag"));
        checkTopologicalSort(dag);
    }

    @Test
    public void test() {
        DAG dag = DAGParser.parseDAG(new File("dags/test.dag"));
        checkTopologicalSort(dag);
    }

    @Test
    public void cybershake30() {
        DAG dag = DAGParser.parseDAG(new File("dags/CyberShake_30.dag"));
        checkTopologicalSort(dag);
    }

    void checkTopologicalSort(DAG dag) {
        // Compute the topological order
        TopologicalOrder order = new TopologicalOrder(dag);

        // Copy to a list
        ArrayList<Task> l = new ArrayList<Task>(dag.numTasks());
        for (Task t : order) {
            l.add(t);
        }

        // Validate that all the children of each task have a sort index later
        // than their parent
        for (String id : dag.getTasks()) {
            Task t = dag.getTask(id);
            int pi = l.indexOf(t);
            for (Task c : t.children) {
                int ci = l.indexOf(c);
                assertTrue(String.format("%s should be after %s", c.id, t.id), ci > pi);
            }
        }
    }
}
