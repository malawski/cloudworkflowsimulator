package cws.core.dag;

import static org.junit.Assert.assertEquals;

import java.io.File;

import org.junit.Test;

public class DAGParserTest {

    @Test
    public void testSmall() {
        DAG dag = DAGParser.parseDAG(new File("dags/cybershake_small.dag"));
        assertEquals(dag.numTasks(), 25703);
        Task t = dag.getTaskById("ID1_8_6");
        assertEquals(t.getId(), "ID1_8_6");
        assertEquals(t.getInputFiles().size(), 3);
        assertEquals(t.getOutputFiles().size(), 2);
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
        Task t = dag.getTaskById("ID00022");
        assertEquals(t.getId(), "ID00022");
        assertEquals(2, t.getInputFiles().size());
        assertEquals(2, t.getOutputFiles().size());
    }
}
