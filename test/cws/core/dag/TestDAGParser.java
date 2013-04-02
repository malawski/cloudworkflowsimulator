package cws.core.dag;

import static org.junit.Assert.*;

import java.io.File;

import org.junit.Test;

import cws.core.dag.DAG;
import cws.core.dag.DAGParser;
import cws.core.dag.Task;

public class TestDAGParser {

    @Test
    public void testSmall() {
        DAG dag = DAGParser.parseDAG(new File("dags/cybershake_small.dag"));
        assertEquals(dag.numTasks(), 25703);
        Task t = dag.getTask("ID1_8_6");
        assertEquals(t.id, "ID1_8_6");
        assertEquals(t.inputs.size(), 3);
        assertEquals(t.outputs.size(), 2);
    }

    @Test
    public void testTiny() {
        DAG dag = DAGParser.parseDAG(new File("dags/test.dag"));
        assertEquals(4, dag.numTasks());
    }

    @Test
    public void testDAX() {
        DAG dag = DAGParser.parseDAX(new File("dags/Montage_25.xml"));
        assertEquals(25, dag.numTasks());
        assertEquals(38, dag.numFiles());
        Task t = dag.getTask("ID00022");
        assertEquals(t.id, "ID00022");
        assertEquals(2, t.inputs.size());
        assertEquals(2, t.outputs.size());
    }
}
