package cws.core.dag;

import static org.junit.Assert.assertEquals;

import java.io.File;

import org.junit.Test;

public class DAGParserTest {
    @Test
    public void testSmall() {
        DAG dag = DAGParser.parseDAG(new File("dags/psmerge_small.dag"));
        assertEquals(96, dag.numTasks());
        Task t = dag.getTaskById("merge1.70");
        assertEquals("merge1.70", t.getId());
        assertEquals(2, t.getInputFiles().size());
        assertEquals(1, t.getOutputFiles().size());
    }

    @Test
    public void testTiny() {
        DAG dag = DAGParser.parseDAG(new File("dags/test1.dag"));
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
