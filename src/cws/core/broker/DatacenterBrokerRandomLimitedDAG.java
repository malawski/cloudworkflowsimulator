package cws.core.broker;


import java.util.Random;


import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.DatacenterBroker;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.SimEvent;
import org.cloudbus.cloudsim.lists.VmList;

import cws.core.dag.Job;

public class DatacenterBrokerRandomLimitedDAG extends DatacenterBroker {

	private int cloudletsSubmitted;
	private Random random;
	private Job job;
	private int maxInStage;

	public DatacenterBrokerRandomLimitedDAG(String name, int maxInStage) throws Exception {
		super(name);
		cloudletsSubmitted=0;
		this.maxInStage = maxInStage;
		random = new Random();
	}
	
    public Job getJob() {
		return job;
	}

	public void setJob(Job job) {
		this.job = job;
	}


	/**
     * Submit cloudlets to the created VMs.
     *
     * @pre $none
     * @post $none
     */
	protected void submitCloudlets() {
		int vmIndex = 0;
		for (Cloudlet cloudlet : job.getEligibleCloudlets()) {
			
			if (getCloudletSubmittedList().size()>=maxInStage) break;
			
			vmIndex = random.nextInt(getVmsCreatedList().size());
			Vm vm;
			if (cloudlet.getVmId() == -1) { //if user didn't bind this cloudlet and it has not been executed yet
				vm = getVmsCreatedList().get(vmIndex);
			} else { //submit to the specific vm
				vm = VmList.getById(getVmsCreatedList(), cloudlet.getVmId());
				if (vm == null) { // vm was not created
					Log.printLine(CloudSim.clock()+": "+getName()+ ": Postponing execution of cloudlet "+cloudlet.getCloudletId()+": bount VM not available");
					continue;
				}
			}

			Log.printLine(CloudSim.clock()+": "+getName()+ ": Sending cloudlet "+cloudlet.getCloudletId()+" to VM #"+vm.getId());
			cloudlet.setVmId(vm.getId());
			sendNow(getVmsToDatacentersMap().get(vm.getId()), CloudSimTags.CLOUDLET_SUBMIT, cloudlet);
			cloudletsSubmitted++;
			getCloudletSubmittedList().add(cloudlet);
			job.setUneligible(cloudlet);
		}

		// remove submitted cloudlets from waiting list
		for (Cloudlet cloudlet : getCloudletSubmittedList()) {
			getCloudletList().remove(cloudlet);
		}
	}	
	
	/**
	 * Process a cloudlet return event.
	 *
	 * @param ev a SimEvent object
	 *
	 * @pre ev != $null
	 * @post $none
	 */
	protected void processCloudletReturn(SimEvent ev) {
		Cloudlet cloudlet = (Cloudlet) ev.getData();
		getCloudletReceivedList().add(cloudlet);
		getCloudletSubmittedList().remove(cloudlet);
		Log.printLine(CloudSim.clock()+": "+getName()+ ": Cloudlet "+cloudlet.getCloudletId()+" received");
		cloudletsSubmitted--;
		job.processCloudletReturn(cloudlet);
		if (getCloudletList().size()==0&&cloudletsSubmitted==0) { //all cloudlets executed
			Log.printLine(CloudSim.clock()+": "+getName()+ ": All Cloudlets executed. Finishing...");
			clearDatacenters();
			finishExecution();
		} else { //some cloudlets haven't finished yet
			Log.printLine(CloudSim.clock()+": "+getName()+ ": Submitting again, cloudlets submitted: " + cloudletsSubmitted);
			Log.printLine(CloudSim.clock()+": "+getName()+ ": Number of finished cloudlets: " + getCloudletReceivedList().size());
			submitCloudlets();
			
			if (getCloudletList().size()>0 && cloudletsSubmitted==0) {
				//all the cloudlets sent finished. It means that some bount
				//cloudlet is waiting its VM be created
				clearDatacenters();
				createVmsInDatacenter(0);
			}

		}
	}
	
	/**
	 * Send an internal event communicating the end of the simulation.
	 *
	 * @pre $none
	 * @post $none
	 */
	private void finishExecution() {
		sendNow(getId(), CloudSimTags.END_OF_SIMULATION);
	}

}
