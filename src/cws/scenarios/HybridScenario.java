package cws.scenarios;



import java.util.List;


import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.core.CloudSim;

import cws.core.PublicDatacenter;

/**
 * A simple example extended to 2 data centers (public, private) 
 * and running jobs (cloudlets) on them.
 * 
 */
public class HybridScenario extends Scenario{


	public HybridScenario() {
		super();
		// Auto-generated constructor 
	}
 

	private PublicDatacenter datacenter1;
	public PublicDatacenter getDatacenter1() {
		return datacenter1;
	}
	public void setDatacenter1(PublicDatacenter datacenter1) {
		this.datacenter1 = datacenter1;
	}


	/**
	 * 
	 * @param numCloudlets - number of cloudlets to simulate
	 * @return debts of user in public datacenter
	 */
	public  double simulate() {

		Log.printLine("Starting CloudSimExample2...");

		try {

			int brokerId = broker.getId();

			
			// submit vm list to the broker
			broker.submitVmList(vmlist);


			// submit cloudlet list to the broker
			broker.submitCloudletList(cloudletList);
			

			// Starts the simulation
			CloudSim.startSimulation();

			CloudSim.stopSimulation();

			// Print results when simulation is over
			List<Cloudlet> newList = broker.getCloudletReceivedList();
			Helper.printCloudletList(newList, name);

			// Print the debt of each user to each datacenter
			double debts = datacenter0.getUserDebts(brokerId);
			String indent = "    ";
			Log.printLine("Debts of user" + indent + brokerId + indent + "are" + indent + debts);

			Log.printLine("Hybrid Scenario finished!");
			return debts;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}




}
