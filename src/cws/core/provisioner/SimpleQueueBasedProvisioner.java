package cws.core.provisioner;

import java.util.Iterator;
import java.util.Set;

import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.core.CloudSim;

import cws.core.Provisioner;
import cws.core.VM;
import cws.core.WorkflowEngine;
import cws.core.WorkflowEvent;

public class SimpleQueueBasedProvisioner extends AbstractProvisioner implements Provisioner, WorkflowEvent {
    
	@Override
	public void provisionResources(WorkflowEngine engine) {
		
		// use the queued (released) jobs from the workflow engine
		int queueLength = engine.getQueueLength();
				
		Log.printLine(CloudSim.clock() + " Provisioner: queue length: " + queueLength);

		
		// check the deadline and budget constraints
		
		double deadline = engine.getDeadline();
		double budget = engine.getBudget();
		double time = CloudSim.clock();
		double cost = engine.getCost();
		
		// if we are close to the budget by one VM*hour
		if ( budget <= cost || time > deadline) {
			return;
		}

		// add one VM if queue not empty		
		if (queueLength>0) {
			VM vm = new VM(1000, 1, 1.0, 1.0);
			Log.printLine(CloudSim.clock() + " Starting VM: " + vm.getId());
			CloudSim.send(engine.getId(), engine.getCloud().getId(), 0.0, VM_LAUNCH, vm);
		} else { // terminate free VMs
			Set<VM> freeVMs = engine.getFreeVMs();
			Iterator<VM> vmIt = freeVMs.iterator();
			while (vmIt.hasNext()) {
				VM vm = vmIt.next();
				vmIt.remove();
				Log.printLine(CloudSim.clock() + " Terminating VM: " + vm.getId());
				CloudSim.send(engine.getId(), engine.getCloud().getId(), 0.0, VM_TERMINATE, vm);
			}
		}
		CloudSim.send(engine.getId(), engine.getId(), PROVISIONER_INTERVAL, PROVISIONING_REQUEST, null);

	}


}
