package cws.core.provisioner;

import java.util.Iterator;
import java.util.Set;

import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.core.CloudSim;

import cws.core.Cloud;
import cws.core.Provisioner;
import cws.core.VM;
import cws.core.WorkflowEngine;
import cws.core.WorkflowEvent;

public class SimpleUtilizationBasedProvisioner extends AbstractProvisioner implements Provisioner, WorkflowEvent {

	// maximum autoscaling factor over initial number of provisioned VMs 
	private static final double MAX_SCALING = 2.0;
	// above this utilization threshold we start provisioning additional VMs
	private static final double UPPER_THRESHOLD=0.90;
	// below this utilization threshold we start deprovisioning vms
	private static final double LOWER_THRESHOLD = 0.70;
	
	private int initialNumVMs = 0;

	@Override
	public void provisionResources(WorkflowEngine engine) {
		
		// when called for the first time it should obtain the initial number of VMs
		if (initialNumVMs==0) initialNumVMs = engine.getAvailableVMs().size();
		
		// check the deadline and budget constraints
		
		double deadline = engine.getDeadline();
		double budget = engine.getBudget();
		double time = CloudSim.clock();
		double cost = engine.getCost();

		Log.printLine(CloudSim.clock() + " Provisioner: Budget consumed " + cost);

		// assuming all VMs are homogeneous
		double vmPrice = 0;
		if (!engine.getAvailableVMs().isEmpty()) vmPrice = engine.getAvailableVMs().get(0).getPrice();
		
		// if we are close to the budget by one VM*hour
		if ( budget - cost <= vmPrice || time > deadline) {
			
			terminateInstances(engine, engine.getFreeVMs());
			terminateInstances(engine, engine.getBusyVMs());
			
			// some instances may be still running so we want to be invoked again to stop them before they reach full hour
			if (engine.getFreeVMs().size()+engine.getBusyVMs().size()>0)
				CloudSim.send(engine.getId(), engine.getId(), PROVISIONER_INTERVAL, PROVISIONING_REQUEST, null);
			// return without further provisioning
			return;
		}
		
		// compute utilization
		double numFreeVMS = engine.getFreeVMs().size();
		double numBusyVMs = engine.getBusyVMs().size();		
		double utilization = numBusyVMs / (numFreeVMS + numBusyVMs);
				
		Log.printLine(CloudSim.clock() + " Provisioner: utilization: " + utilization);

		if (utilization > UPPER_THRESHOLD && numBusyVMs+numFreeVMS < MAX_SCALING * initialNumVMs) {
			
			VM vm = new VM(1000, 1, 1.0, 1.0);
			Log.printLine(CloudSim.clock() + " Starting VM: " + vm.getId());
			CloudSim.send(engine.getId(), cloud.getId(), 0.0, VM_LAUNCH, vm);
			
		} else if (utilization < LOWER_THRESHOLD) {
			
			Set<VM> freeVMs = engine.getFreeVMs();
			terminateInstances(engine, freeVMs);	
			
		}
		
		// send event to initiate next provisioning cycle
		CloudSim.send(engine.getId(), engine.getId(), PROVISIONER_INTERVAL, PROVISIONING_REQUEST, null);

	}
	
	/**
	 * This method terminates instances but only the ones 
	 * that are close to the full hour of operation.
	 * Thus this method has to be invoked several times 
	 * to effectively terminate all the instances.
	 * 
	 * @param engine
	 * @param vmSet
	 */
	
	private void terminateInstances(WorkflowEngine engine, Set<VM> vmSet) {
		
		Iterator<VM> vmIt = vmSet.iterator();
		
		while (vmIt.hasNext()) {
			VM vm = vmIt.next();
			double vmRuntime = vm.getRuntime();

			// full hours (rounded up)
			double vmHours = Math.ceil(vmRuntime/3600.0);

			// seconds till next full hour
			double secondsRemaining = vmHours*3600.0-vmRuntime;
			
			//terminate only vms that have less seconds remaining than a defined threshold
			if (secondsRemaining<=PROVISIONER_INTERVAL) {
				vmIt.remove();
				Log.printLine(CloudSim.clock() + " Terminating VM: " + vm.getId());
				CloudSim.send(engine.getId(), cloud.getId(), 0.0, VM_TERMINATE, vm);				
			}
		}
	}

}
