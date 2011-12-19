package cws.core;

import java.util.List;
import java.util.Map;
import org.cloudbus.cloudsim.Datacenter;
import org.cloudbus.cloudsim.DatacenterCharacteristics;
import org.cloudbus.cloudsim.Host;
import org.cloudbus.cloudsim.Storage;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.VmAllocationPolicy;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;

/**
 * PublicDatacenter is a class that enables simulation of public and private data centers.
 *
 */
public class PublicDatacenter extends Datacenter {
    
	/**
	 * Instantiates a new datacenter.
	 *
	 * @param name the name
	 * @param characteristics the res config
	 * @param schedulingInterval the scheduling interval
	 * @param utilizationBound the utilization bound
	 * @param vmAllocationPolicy the vm provisioner
	 * @param storageList the storage list
	 *
	 * @throws Exception the exception
	 */
	public PublicDatacenter(
			String name,
			DatacenterCharacteristics characteristics,
			VmAllocationPolicy vmAllocationPolicy,
			List<Storage> storageList,
			double schedulingInterval) throws Exception {
		super(name, characteristics, vmAllocationPolicy, storageList, schedulingInterval);
	}

	/**
	 * Updates processing of each cloudlet running in this Datacenter. It is necessary because
	 * Hosts and VirtualMachines are simple objects, not entities. So, they don't receive events
	 * and updating cloudlets inside them must be called from the outside.
	 *
	 * @pre $none
	 * @post $none
	 */
	@Override
	protected void updateCloudletProcessing() {

		
		double currentTime = CloudSim.clock();
		double timeDiff = currentTime - getLastProcessTime();
		
		if (CloudSim.clock() > this.getLastProcessTime()) {
			List<? extends Host> list = getVmAllocationPolicy().getHostList();
			double smallerTime = Double.MAX_VALUE;
			//for each host...
			for (int i = 0; i < list.size(); i++) {
				Host host = list.get(i);
				double time = host.updateVmsProcessing(CloudSim.clock());//inform VMs to update processing
				//what time do we expect that the next cloudlet will finish?
				if (time < smallerTime) {
					smallerTime = time;
				}
			}
			
			//MM added cost 
			double amount = 0.0;
//			String indent = "    ";
			
			for (Host host : this.<Host>getHostList()) {
				for (Vm vm : host.getVmList()) {
					if (getDebts().containsKey(vm.getUserId())) {
						amount = getDebts().get(vm.getUserId());
					}
//					Log.printLine("Cost per mi" + indent + getCharacteristics().getCostPerMi()
//							+ indent + "mips of pe" + indent + getCharacteristics().getMipsOfOnePe()
//							+ indent + "time diff" + indent + timeDiff);

					amount += getCharacteristics().getCostPerMi() * getCharacteristics().getMipsOfOnePe() * timeDiff;
					getDebts().put(vm.getUserId(), amount);
				}
			}
			
			//end MM
			
			//schedules an event to the next time, if valid
			//if (smallerTime > CloudSim.clock() + 0.01 && smallerTime != Double.MAX_VALUE && smallerTime < getSchedulingInterval()) {
			if (smallerTime > CloudSim.clock() + 0.01 && smallerTime != Double.MAX_VALUE) {
				CloudSim.cancelAll(getId(), CloudSim.SIM_ANY);
				schedule(getId(), (smallerTime - CloudSim.clock()), CloudSimTags.VM_DATACENTER_EVENT);
			}
			setLastProcessTime(CloudSim.clock());
		}
		
	}
	
	public double getUserDebts(int userID) {
		Map<Integer, Double> map = getDebts();
		double debts = map.get(userID);
		return debts;
	}
}
