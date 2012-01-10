package cws.core.dag.algorithms;

import static org.junit.Assert.*;

import java.io.File;

import org.junit.Test;

import cws.core.dag.DAG;
import cws.core.dag.DAGParser;
import cws.core.dag.Task;

public class CriticalPathTest {
    
    @Test
    public void cptest() {
        DAG dag = DAGParser.parseDAG(new File("dags/cptest.dag"));
        TopologicalOrder order = new TopologicalOrder(dag);
        CriticalPath cp = new CriticalPath(order);
        
        Task A = dag.getTask("A");
        Task B = dag.getTask("B");
        Task C = dag.getTask("C");
        Task D = dag.getTask("D");
        Task E = dag.getTask("E");
        
        assertEquals(1.0, cp.eft(A), 0.00001);
        
        assertEquals(2.0, cp.eft(B), 0.00001);
        
        assertEquals(3.0, cp.eft(C), 0.00001);
        
        assertEquals(4.0, cp.eft(D), 0.00001);
        
        assertEquals(5.0, cp.eft(E), 0.00001);
        
        assertEquals(5.0, cp.getCriticalPathLength(), 0.00001);
    }
    
    @Test
    public void test() {
        DAG dag = DAGParser.parseDAG(new File("dags/test.dag"));
        TopologicalOrder order = new TopologicalOrder(dag);
        CriticalPath cp = new CriticalPath(order);
        assertEquals(21, cp.getCriticalPathLength(), 0.00001);
    }
    
    @Test
    public void cybershake30() {
        DAG dag = DAGParser.parseDAG(new File("dags/CyberShake_30.dag"));
        TopologicalOrder order = new TopologicalOrder(dag);
        CriticalPath cp = new CriticalPath(order);
        assertEquals(221.84, cp.getCriticalPathLength(), 0.00001);
    }
}
