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
}
