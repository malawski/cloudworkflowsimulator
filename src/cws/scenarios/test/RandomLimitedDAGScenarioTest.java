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

public class RandomLimitedDAGScenarioTest {

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
		
		scenario.init("randomlimdag1");
		datacenter0 = PublicDatacenterFactory.create("PublicDatacenter_0",0.1/3600,1);
		datacenter1 = PublicDatacenterFactory.create("PublicDatacenter_1",0.2/3600,1);
		scenario.setDatacenter0(datacenter0);
		scenario.setDatacenter1(datacenter1);
		broker	 = BrokerFactory.createBrokerRandomLimitedDAG(5);
		List<Cloudlet> cloudletList = CloudletListGenerator.generateCloudlets(12, 1, 600000, 300, 300, broker.getId());
		scenario.setCloudletList(cloudletList);
		Job job = new Job();
		job.setCloudlets(cloudletList);
		job.generateDag();
		broker.setJob(job);
		scenario.setBroker(broker);
		scenario.setVmlist(VmListGenerator.generateVmList(2, broker.getId()));

		
		Helper.saveDot(job, "randomlimdag1");
		assertEquals(0.1, scenario.simulate(),0.25);
	}

	
	@Test
	public void testSimulate2() {
		
		scenario.init("randomlimdag2");
		datacenter0 = PublicDatacenterFactory.create("PublicDatacenter_0",0.1/3600,1);
		datacenter1 = PublicDatacenterFactory.create("PublicDatacenter_1",0.2/3600,1);
		scenario.setDatacenter0(datacenter0);
		scenario.setDatacenter1(datacenter1);
		broker	 = BrokerFactory.createBrokerRandomLimitedDAG(5);
		List<Cloudlet> cloudletList = CloudletListGenerator.generateCloudlets(24, 1, 600000, 300, 300, broker.getId());
		scenario.setCloudletList(cloudletList);
		Job job = new Job();
		job.setCloudlets(cloudletList);
		job.generateDag();
		broker.setJob(job);
		scenario.setBroker(broker);
		scenario.setVmlist(VmListGenerator.generateVmList(2, broker.getId()));

		Helper.saveDot(job, "randomlimdag2");
		assertEquals(0.2, scenario.simulate(), 0.25);
	}
	
	@Test
	public void testSimulate3() {
		
		scenario.init("randomlimdag3");
		datacenter0 = PublicDatacenterFactory.create("PublicDatacenter_0",0.1/3600,1);
		datacenter1 = PublicDatacenterFactory.create("PublicDatacenter_1",0.2/3600,1);
		scenario.setDatacenter0(datacenter0);
		scenario.setDatacenter1(datacenter1);
		broker	 = BrokerFactory.createBrokerRandomLimitedDAG(5);
		List<Cloudlet> cloudletList = CloudletListGenerator.generateCloudlets(120, 1, 600000, 300, 300, broker.getId());
		scenario.setCloudletList(cloudletList);
		Job job = new Job();
		job.setCloudlets(cloudletList);
		job.generateDag();
		broker.setJob(job);
		scenario.setBroker(broker);
		scenario.setVmlist(VmListGenerator.generateVmList(2, broker.getId()));

		Helper.saveDot(job, "randomlimdag3");
		assertEquals(1.0, scenario.simulate(),0.4);
	}
	
	
	@Test
	public void testSimulate4() {
		
		scenario.init("randomlimdag4");
		datacenter0 = PublicDatacenterFactory.create("PublicDatacenter_0",0.1/3600,10);
		datacenter1 = PublicDatacenterFactory.create("PublicDatacenter_1",0.2/3600,10);
		scenario.setDatacenter0(datacenter0);
		scenario.setDatacenter1(datacenter1);
		broker	 = BrokerFactory.createBrokerRandomLimitedDAG(5);
		List<Cloudlet> cloudletList = CloudletListGenerator.generateCloudlets(120, 1, 600000, 300, 300, broker.getId());
		scenario.setCloudletList(cloudletList);
		Job job = new Job();
		job.setCloudlets(cloudletList);
		job.generateDag();
		broker.setJob(job);
		scenario.setBroker(broker);
		scenario.setVmlist(VmListGenerator.generateVmList(20, broker.getId()));

		Helper.saveDot(job, "randomlimdag4");
		assertEquals(1.0, scenario.simulate(),4.0);
	}
	
	
	@Test
	public void testSimulate5() {
		
		scenario.init("randomlimdag5");
		datacenter0 = PublicDatacenterFactory.create("PublicDatacenter_0",0.1/3600,2);
		datacenter1 = PublicDatacenterFactory.create("PublicDatacenter_1",0.2/3600,2);
		scenario.setDatacenter0(datacenter0);
		scenario.setDatacenter1(datacenter1);
		broker	 = BrokerFactory.createBrokerRandomLimitedDAG(5);
		List<Cloudlet> cloudletList = CloudletListGenerator.generateCloudlets(24, 1, 600000, 300, 300, broker.getId());
		scenario.setCloudletList(cloudletList);
		Job job = new Job();
		job.setCloudlets(cloudletList);
		job.generateDag();
		broker.setJob(job);
		scenario.setBroker(broker);
		scenario.setVmlist(VmListGenerator.generateVmList(4, broker.getId()));

		Helper.saveDot(job, "randomlimdag5");
		assertEquals(0.2, scenario.simulate(), 0.25);
	}
	

}
