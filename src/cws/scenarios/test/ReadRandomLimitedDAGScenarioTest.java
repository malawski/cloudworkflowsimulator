package cws.scenarios.test;

import static org.junit.Assert.*;

import java.util.List;


import org.cloudbus.cloudsim.Cloudlet;
import org.junit.Before;
import org.junit.Test;

import cws.core.PublicDatacenter;
import cws.core.broker.BrokerFactory;
import cws.core.broker.DatacenterBrokerRandomDAG;
import cws.core.broker.DatacenterBrokerRandomLimitedDAG;
import cws.core.dag.Job;
import cws.scenarios.CloudletListGenerator;
import cws.scenarios.Helper;
import cws.scenarios.HybridScenario;
import cws.scenarios.PublicDatacenterFactory;
import cws.scenarios.VmListGenerator;

public class ReadRandomLimitedDAGScenarioTest {

	HybridScenario scenario;
	PublicDatacenter datacenter0;
	PublicDatacenter datacenter1;
	DatacenterBrokerRandomLimitedDAG broker;
	
	@Before
	public void setUp() {
		scenario = new HybridScenario();
	}
	
	@Test
	public void testSimulate1() {
		
		scenario.init("readlimdag1");
		datacenter0 = PublicDatacenterFactory.create("PublicDatacenter_0",0.1/3600,128);
		datacenter1 = PublicDatacenterFactory.create("PublicDatacenter_1",0.2/3600,128);
		scenario.setDatacenter0(datacenter0);
		scenario.setDatacenter1(datacenter1);
		broker	 = BrokerFactory.createBrokerRandomLimitedDAG(512);
		Job job = new Job();
		// assigning cloudlets to broker should be done probably later
		job.readDag(broker.getId(), "dags/cybershake_small.dag");
		scenario.setCloudletList(job.getCloudlets());
		broker.setJob(job);
		scenario.setBroker(broker);
		scenario.setVmlist(VmListGenerator.generateVmList(256, broker.getId()));
//		Helper.saveDot(job, "readdag1");
		assertEquals(3.1, scenario.simulate(),1.25);
	}

	


}
