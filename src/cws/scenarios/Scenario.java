package cws.scenarios;

import java.util.Calendar;
import java.util.List;


import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.DatacenterBroker;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.core.CloudSim;

import cws.core.PublicDatacenter;

public class Scenario {

	protected String name;
	
	protected PublicDatacenter datacenter0;

	public void init(String name) {
		// First step: Initialize the CloudSim package. It should be called
		// before creating any entities.
		this.name = name;
		int num_user = 1; // number of cloud users
		Calendar calendar = Calendar.getInstance();
		boolean trace_flag = false; // mean trace events
	
		// Initialize the CloudSim library
		CloudSim.init(num_user, calendar, trace_flag);
		
	}

	public List<Vm> getVmlist() {
		return vmlist;
	}

	public void setVmlist(List<Vm> vmlist) {
		this.vmlist = vmlist;
	}

	protected DatacenterBroker broker;
	/** The cloudlet list. */
	protected List<Cloudlet> cloudletList;
	
	public List<Cloudlet> getCloudletList() {
		return cloudletList;
	}

	public void setCloudletList(List<Cloudlet> cloudletList) {
		this.cloudletList = cloudletList;
	}

	/** The vmlist. */
	protected List<Vm> vmlist;



	public DatacenterBroker getBroker() {
		return broker;
	}

	public void setBroker(DatacenterBroker broker) {
		this.broker = broker;
	}

	public PublicDatacenter getDatacenter0() {
		return datacenter0;
	}

	public void setDatacenter0(PublicDatacenter datacenter0) {
		this.datacenter0 = datacenter0;
	}

}