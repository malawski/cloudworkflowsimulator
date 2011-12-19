package cws.core.scheduler;

import java.util.Iterator;
import java.util.List;
import java.util.Queue;

import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.core.CloudSim;

import cws.core.Job;
import cws.core.Scheduler;
import cws.core.VM;
import cws.core.WorkflowEngine;
import cws.core.WorkflowEvent;


/** This scheduler submits jobs to VMs on FCFS basis.
 *  The ready jobs are inserted into VM queues for execution.
 * @author malawski
 *
 */

public class DAGSchedulerFCFS implements Scheduler, WorkflowEvent {

	private List<VM> vms;

	@Override
	public void scheduleJobs(WorkflowEngine engine) {

		Queue<Job> jobs = engine.getQueuedJobs();
		vms = engine.getAvailableVMs();
		
		// if there is nothing to do, just return
		if (vms.isEmpty()) return;
		if (jobs.isEmpty()) return;

		Iterator<VM> vmIt = vms.iterator();
		while (!jobs.isEmpty() && vmIt.hasNext()) {
				VM vm = vmIt.next();
				if (vm.getQueueLength()==0) {
					Job job = jobs.poll(); // retrieve and remove job from ready set
					job.setVM(vm);
					Log.printLine(CloudSim.clock() + " Submitting job " + job.getID() + " to VM " + job.getVM().getId());
					CloudSim.send(engine.getId(), vm.getId(), 0.0, JOB_SUBMIT, job);	
				}
		}			
	}
	

}
