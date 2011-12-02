package cws.core.scheduler;

import java.util.Comparator;
import java.util.HashSet;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;

import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.core.CloudSim;

import cws.core.DAGJob;
import cws.core.Job;
import cws.core.VM;
import cws.core.WorkflowEngine;
import cws.core.dag.algorithms.Characteristics;


/** This scheduler submits workflow ensemble to VMs on FCFS basis.
 *  Job is submitted to VM only if VM is idle (no queueing in VMs) 
 *  and if there are no higher priority jobs in the queue. 
 * @author malawski
 *
 */

public class WorkflowAwareEnsembleScheduler extends EnsembleDynamicScheduler {
	
	
	private Set<DAGJob> startedDAGs = new HashSet<DAGJob>();
	private Set<DAGJob> rejectedDAGs = new HashSet<DAGJob>();

	@Override
	public void scheduleJobs(WorkflowEngine engine) {
		
		// check the deadline constraints (provisioner takes care about budget)
		
		double deadline = engine.getDeadline();
		double time = CloudSim.clock();		
		
		// stop scheduling any new jobs if we are over deadline
		if (time >= deadline) {
			return;
		}
		

		Queue<Job> jobs = engine.getQueuedJobs();
		
		// move all jobs to priority queue
		
		while (!jobs.isEmpty()) {
			Job job = jobs.poll();
			DAGJob dj = job.getDAGJob();
			
			if (rejectedDAGs.contains(dj)) {
				// skip this job
			} else if (startedDAGs.contains(dj)) {
				// schedule the job
				prioritizedJobs.add(job);
			}
			
			else if (admitDAG(dj, engine))
			{
					prioritizedJobs.add(job);
					startedDAGs.add(dj);
			} else {
					rejectedDAGs.add(dj);
				
			}

		}
			// use prioritized list for scheduling
		scheduleQueue(prioritizedJobs, engine);	
		
		// update queue length for the provisioner
		engine.setQueueLength(prioritizedJobs.size());
		
	}
		

		
	// decide what to do with the job from a new dag
	private boolean admitDAG(DAGJob dj, WorkflowEngine engine) {
			
		double costEstimate = estimateCost(dj, engine);	
		double budgetRemaining = estimateBudgetRemaining(dj, engine);
		Log.printLine(CloudSim.clock() + " Cost estimate: " + costEstimate + " Budget remaining: " + budgetRemaining);

		return costEstimate<budgetRemaining ;
	}


	/** 
	 * Estimate cost of this workflow
	 * @param dj
	 * @param engine
	 * @return
	 */
	private double estimateCost(DAGJob dj, WorkflowEngine engine) {
		
		Characteristics c = new Characteristics(dj.getDAG());
		double sumRuntime = c.sumRuntime();
		// assume that all vms are homogeneous
		double vmPrice = 0;
		if (!engine.getAvailableVMs().isEmpty()) vmPrice = engine.getAvailableVMs().get(0).getPrice();
		return vmPrice*sumRuntime/3600.0;
	}

	/**
	 * Estimate budget remaining, including unused $ and running VMs
	 * TODO: compute budget consumed/remaining by already admitted workflows
	 * @param dj
	 * @param engine
	 * @return
	 */
	private double estimateBudgetRemaining(DAGJob dj, WorkflowEngine engine) {

		// remaining budget for starting new vms 
		double rn = engine.getBudget()-engine.getCost();

		// compute remaining (cot consumed) budget of currently running VMs
		double rc = 0.0;
		
		Set<VM> vms = new HashSet<VM>();
		vms.addAll(engine.getFreeVMs());
		vms.addAll(engine.getBusyVMs());
		
		for (VM vm : vms) {
			rc += vm.getCost() - vm.getRuntime()*vm.getPrice()/3600.0;
		}
		
		// we add this for safety in order not to underestimate our budget
		double safetyMargin = 1.0;
		
		return rn+rc-safetyMargin;
	}

	

}
