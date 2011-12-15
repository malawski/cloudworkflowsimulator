package cws.core.experiment;

import static org.junit.Assert.*;

import org.junit.Test;

public class DAGListGeneratorTest {

	@Test
	public void testGenerateDAGList() {
		String[] dags = DAGListGenerator.generateDAGList("SIPHT", new int [] {900,1000}, 20);
		assertEquals("SIPHT.n.900.0.dax", dags[0]);
		assertEquals("SIPHT.n.1000.0.dax", dags[20]);
		assertEquals("SIPHT.n.1000.19.dax", dags[39]);	
	}
}
