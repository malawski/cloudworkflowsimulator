package cws.core.dag.algorithms;

import static org.junit.Assert.*;

import java.io.File;

import org.junit.Test;

import cws.core.dag.DAG;
import cws.core.dag.DAGParser;
import cws.core.dag.Task;

public class CriticalPathTest {

	@Test
	public void testEft() {
		DAG dag = DAGParser.parseDAG(new File("dags/test.dag"));
        TopologicalOrder order = new TopologicalOrder(dag);
        for (Task task : order.order()) System.out.print(task + " "); System.out.println();
        CriticalPath cp = new CriticalPath(dag, order);
        for (Task task : order.order()) {
        	System.out.println(task + "\t" + task.size + "\t" + cp.est(task) + "\t" + cp.eft(task));
        }
        System.out.println("Critical path: " + cp.getCriticalPathLength());
	}
	
	@Test
	public void testEft30() {
		DAG dag = DAGParser.parseDAG(new File("dags/CyberShake_30.dag"));
        TopologicalOrder order = new TopologicalOrder(dag);
        for (Task task : order.order()) System.out.print(task + " "); System.out.println();
        CriticalPath cp = new CriticalPath(dag, order);
        for (Task task : order.order()) {
        	System.out.println(task + "\t" + task.size + "\t" + cp.est(task) + "\t" + cp.eft(task));
        }
        System.out.println("Critical path: " + cp.getCriticalPathLength());
	}

}
