package cws.core.experiment;

import static org.junit.Assert.*;

import java.util.Random;

import org.junit.Test;

public class DAGListGeneratorTest {

	@Test
	public void testGenerateDAGList() {
		String[] dags = DAGListGenerator.generateDAGList("SIPHT", new int [] {900,1000}, 20);
		assertEquals("SIPHT.n.900.0.dag", dags[0]);
		assertEquals("SIPHT.n.1000.0.dag", dags[20]);
		assertEquals("SIPHT.n.1000.19.dag", dags[39]);	
	}
	
	@Test
	public void testPareto() {
		String[] dags = DAGListGenerator.generateDAGListPareto(new Random(0), "SIPHT", 20);
		assertEquals("SIPHT.n.1000.0.dag", dags[0]);
		assertEquals("SIPHT.n.50.0.dag", dags[19]);
		assertEquals("SIPHT.n.300.1.dag", dags[2]);	
	}
	
	@Test
	public void testPareto40() {
		String[] dags = DAGListGenerator.generateDAGListPareto(new Random(0), "SIPHT", 40);
		assertEquals("SIPHT.n.1000.1.dag", dags[0]);
		assertEquals("SIPHT.n.50.0.dag", dags[39]);
		assertEquals("SIPHT.n.300.1.dag", dags[7]);	
	}
	
	@Test
	public void testConstant40() {
		String[] dags = DAGListGenerator.generateDAGListConstant("SIPHT", 1000, 40);
		assertEquals("SIPHT.n.1000.0.dag", dags[0]);
		assertEquals("SIPHT.n.1000.19.dag", dags[39]);
		assertEquals("SIPHT.n.1000.7.dag", dags[7]);	
	}
}
