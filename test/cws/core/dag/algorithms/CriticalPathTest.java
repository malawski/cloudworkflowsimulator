package cws.core.dag.algorithms;

import static org.junit.Assert.assertEquals;

import java.io.File;

import org.junit.Before;
import org.junit.Test;

import cws.core.cloudsim.CloudSimWrapper;
import cws.core.core.VMType;
import cws.core.core.VMTypeBuilder;
import cws.core.dag.DAG;
import cws.core.dag.DAGParser;
import cws.core.dag.Task;
import cws.core.engine.Environment;
import cws.core.engine.StorageAwarePredictionStrategy;
import cws.core.storage.VoidStorageManager;

public class CriticalPathTest {
    private Environment environment;
    private CloudSimWrapper cloudsim;

    @Before
    public void setUp() {
        cloudsim = new CloudSimWrapper();
        cloudsim.init();

        VMType vmType = VMTypeBuilder.newBuilder().mips(1).cores(1).price(1.0).build();
        VoidStorageManager storageManager = new VoidStorageManager(cloudsim);
        environment = new Environment(vmType, storageManager, new StorageAwarePredictionStrategy());
    }

    @Test
    public void cptest() {
        DAG dag = DAGParser.parseDAG(new File("dags/cptest.dag"));
        TopologicalOrder order = new TopologicalOrder(dag);
        CriticalPath cp = new CriticalPath(order, environment);

        Task A = dag.getTaskById("A");
        Task B = dag.getTaskById("B");
        Task C = dag.getTaskById("C");
        Task D = dag.getTaskById("D");
        Task E = dag.getTaskById("E");

        assertEquals(1.0, cp.getEarliestFinishTime(A), 0.00001);

        assertEquals(2.0, cp.getEarliestFinishTime(B), 0.00001);

        assertEquals(3.0, cp.getEarliestFinishTime(C), 0.00001);

        assertEquals(4.0, cp.getEarliestFinishTime(D), 0.00001);

        assertEquals(5.0, cp.getEarliestFinishTime(E), 0.00001);

        assertEquals(5.0, cp.getCriticalPathLength(), 0.00001);
    }

    @Test
    public void test() {
        DAG dag = DAGParser.parseDAG(new File("dags/test.dag"));
        TopologicalOrder order = new TopologicalOrder(dag);
        CriticalPath cp = new CriticalPath(order, environment);
        assertEquals(21, cp.getCriticalPathLength(), 0.00001);
    }

    @Test
    public void cybershake30() {
        DAG dag = DAGParser.parseDAG(new File("dags/CyberShake_30.dag"));
        TopologicalOrder order = new TopologicalOrder(dag);
        CriticalPath cp = new CriticalPath(order, environment);
        assertEquals(221.84, cp.getCriticalPathLength(), 0.00001);
    }
}
