package cws.core.dag.algorithms;

import java.io.File;
import java.util.Arrays;

import org.junit.Test;
import static org.junit.Assert.*;



import cws.core.dag.DAG;
import cws.core.dag.DAGParser;
import cws.core.dag.Task;


public class TopologicalOrderTest {
   
    @Test
    public void topologicalTest() {
        DAG dag = DAGParser.parseDAG(new File("dags/test.dag"));
        TopologicalOrder order = new TopologicalOrder(dag);
        System.out.println(order.postorder());
        for (Task task : order.reversePostorder()) System.out.print(task + " ");
        System.out.println();
    }

    @Test
    public void topologicalTest30() {
        DAG dag = DAGParser.parseDAG(new File("dags/CyberShake_30.dag"));
        TopologicalOrder order = new TopologicalOrder(dag);
        System.out.println(order.postorder());
        for (Task task : order.reversePostorder()) System.out.print(task + " ");
        System.out.println();
        for (Task task : order.postorder()) {
        	checkOrder(task, order.postorder());        	
        }      
    }
   
    void checkOrder(Task task, Iterable<Task> postorder) {
        boolean before = true;       
        for (Task t : postorder) {
            if (t==task) {
                before = false;
            } else if (before) {
                assertFalse(task.parents.contains(t));
            } else {
            	assertFalse(task.children.contains(t));
            }
        }
       
    }
   
}

