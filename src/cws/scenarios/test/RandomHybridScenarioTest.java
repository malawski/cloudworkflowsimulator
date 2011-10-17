package cws.scenarios.test;

import static org.junit.Assert.*;


import org.cloudbus.cloudsim.DatacenterBroker;
import org.junit.Before;
import org.junit.Test;

import cws.core.PublicDatacenter;
import cws.core.broker.BrokerFactory;
import cws.scenarios.CloudletListGenerator;
import cws.scenarios.HybridScenario;
import cws.scenarios.PublicDatacenterFactory;
import cws.scenarios.VmListGenerator;

public class RandomHybridScenarioTest {

	HybridScenario scenario;
	PublicDatacenter datacenter0;
	PublicDatacenter datacenter1;
	DatacenterBroker broker;
	
	@Before
	public void setUp() {
		scenario = new HybridScenario();
	}
	
	@Test
	public void testSimulate1() {
		
		scenario.init("random1");
		datacenter0 = PublicDatacenterFactory.create("PublicDatacenter_0",0.1/3600,1);
		datacenter1 = PublicDatacenterFactory.create("PublicDatacenter_1",0.2/3600,1);
		scenario.setDatacenter0(datacenter0);
		scenario.setDatacenter1(datacenter1);
		broker	 = BrokerFactory.createBrokerRandom();
		scenario.setBroker(broker);
		scenario.setCloudletList(CloudletListGenerator.generateCloudlets(12, 1, 600000, 300, 300, broker.getId())); 
		scenario.setVmlist(VmListGenerator.generateVmList(2, broker.getId()));

		assertEquals(0.1, scenario.simulate(),0.2);
	}
	
	@Test
	public void testSimulate2() {	
		scenario.init("random2");
		datacenter0 = PublicDatacenterFactory.create("PublicDatacenter_0",0.1/3600,1);
		datacenter1 = PublicDatacenterFactory.create("PublicDatacenter_1",0.2/3600,1);
		scenario.setDatacenter0(datacenter0);
		scenario.setDatacenter1(datacenter1);
		broker	 = BrokerFactory.createBrokerRandom();
		scenario.setBroker(broker);
		scenario.setCloudletList(CloudletListGenerator.generateCloudlets(24, 1, 600000, 300, 300, broker.getId())); 
		scenario.setVmlist(VmListGenerator.generateVmList(2, broker.getId()));

		assertEquals(0.2, scenario.simulate(), 0.2);
	}
	
	@Test
	public void testSimulate3() {
		scenario.init("random3");
		datacenter0 = PublicDatacenterFactory.create("PublicDatacenter_0",0.1/3600,1);
		datacenter1 = PublicDatacenterFactory.create("PublicDatacenter_1",0.2/3600,1);
		scenario.setDatacenter0(datacenter0);
		scenario.setDatacenter1(datacenter1);
		broker	 = BrokerFactory.createBrokerRandom();
		scenario.setBroker(broker);
		scenario.setCloudletList(CloudletListGenerator.generateCloudlets(120, 1, 600000, 300, 300, broker.getId())); 
		scenario.setVmlist(VmListGenerator.generateVmList(2, broker.getId()));

		assertEquals(1.0, scenario.simulate(),0.2);
	}
	


}
