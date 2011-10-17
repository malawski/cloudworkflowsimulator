package cws.scenarios;

import java.util.ArrayList;
import java.util.List;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.UtilizationModel;
import org.cloudbus.cloudsim.UtilizationModelFull;
import org.cloudbus.cloudsim.distributions.ExponentialDistr;

public class CloudletListGenerator {

	private static ExponentialDistr distribution = null;
	
	public static ExponentialDistr getDistribution() {
		if (distribution==null) distribution = new ExponentialDistr(1);
		return distribution;
	}
	
	public static List<Cloudlet>  generateCloudlets(int numCloudlets, int pesNumber, long length, long fileSize, long outputSize, int brokerId) {
	
	List<Cloudlet> cloudletList  = new ArrayList<Cloudlet>();
		
	for (int i=0;i<numCloudlets; i++) {
		
		
		// Cloudlet properties
		int id = i;

		UtilizationModel utilizationModel = new UtilizationModelFull();

		Cloudlet cloudlet = new Cloudlet(id, length, pesNumber, fileSize, outputSize, utilizationModel, utilizationModel, utilizationModel);
		cloudlet.setUserId(brokerId);
//		cloudlet.setVmId(vmid);
		// add the cloudlet to the list
		cloudletList.add(cloudlet);
	}
	return cloudletList;
	
	}
	
	
	public static List<Cloudlet>  generateCloudletsFromDistribution(int numCloudlets, int pesNumber, long length, long fileSize, long outputSize, int brokerId) {
	
	List<Cloudlet> cloudletList  = new ArrayList<Cloudlet>();
	
	ExponentialDistr distribution = getDistribution();
		
	for (int i=0;i<numCloudlets; i++) {
		
		
		// Cloudlet properties
		int id = i;

		UtilizationModel utilizationModel = new UtilizationModelFull();

		Cloudlet cloudlet = new Cloudlet(id, (int) (length*distribution.sample()), pesNumber, fileSize, outputSize, utilizationModel, utilizationModel, utilizationModel);
		cloudlet.setUserId(brokerId);
//		cloudlet.setVmId(vmid);
		// add the cloudlet to the list
		cloudletList.add(cloudlet);
	}
	return cloudletList;
	
	}
	
}
