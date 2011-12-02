package cws.core.scheduler;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Queue;
import java.util.Set;

import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.core.CloudSim;

import cws.core.Job;
import cws.core.JobListener;
import cws.core.Scheduler;
import cws.core.VM;
import cws.core.VMListener;
import cws.core.WorkflowEngine;
import cws.core.WorkflowEvent;


/** This scheduler submits jobs to VMs on FCFS basis.
 *  Job is submitted to VM only if VM is idle (no queueing in VMs). 
 * @author malawski
 *
 */

public class DAGDynamicScheduler implements Scheduler, WorkflowEvent {
	

	@Override
	public void scheduleJobs(WorkflowEngine engine) {

		// use the queued (released) jobs from the workflow engine
		Queue<Job> jobs = engine.getQueuedJobs();
		
		scheduleQueue(jobs, engine);		
	}


    /**
     * Schedule all jobs from the queue to available free VMs.
     * Successfully scheduled jobs are removed from the queue. 
     * @param jobs
     * @param engine
     */
	protected void scheduleQueue(Queue<Job> jobs, WorkflowEngine engine) {

		Set<VM> freeVMs = engine.getFreeVMs();
		Set<VM> busyVMs = engine.getBusyVMs();
		
		while (!freeVMs.isEmpty() && !jobs.isEmpty()) {
			Job job = jobs.poll();
			VM vm = freeVMs.iterator().next();
			job.setVM(vm);
			freeVMs.remove(vm); // remove VM from free set
			busyVMs.add(vm); //add vm to busy set
			Log.printLine(CloudSim.clock() + " Submitting job " + job.getID() + " to VM " + job.getVM().getId());
			CloudSim.send(engine.getId(), vm.getId(), 0.0, JOB_SUBMIT, job);	
		}
	}

}
