package cws.core.scheduler;

import java.util.HashSet;
import java.util.Queue;
import java.util.Set;

import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.core.CloudSim;

import cws.core.DAGJob;
import cws.core.Job;
import cws.core.VM;
import cws.core.WorkflowEngine;
import cws.core.dag.DAG;
import cws.core.dag.Task;


/** 
 * This scheduler submits workflow ensemble to VMs on FCFS basis.
 * Job is submitted to VM only if VM is idle (no queueing in VMs) 
 * and if there are no higher priority jobs in the queue.
 *  
 * @author malawski
 */
public class WorkflowAwareEnsembleScheduler extends EnsembleDynamicScheduler {
	private Set<DAGJob> admittedDAGs = new HashSet<DAGJob>();
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
		prioritizedJobs.addAll(jobs);
		jobs.clear();
		

			// use prioritized list for scheduling
		scheduleQueue(prioritizedJobs, engine);	
		
		// update queue length for the provisioner
		engine.setQueueLength(prioritizedJobs.size());
		
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
			
			
			// remove first job from the prioroty queue if dag not admitted
			
			DAGJob dj = job.getDAGJob();
			
			if (rejectedDAGs.contains(dj)) {
				// ignore
				return;
			} else if (admittedDAGs.contains(dj)) {
				// schedule the job
			} else if (admitDAG(dj, engine)) {
				// if the DAG is admitted we add it to the queue
				admittedDAGs.add(dj);
			} else {
				rejectedDAGs.add(dj);
				// skip this job
				return;
			}
			
			
			VM vm = freeVMs.iterator().next();
			job.setVM(vm);
			freeVMs.remove(vm); // remove VM from free set
			busyVMs.add(vm); //add vm to busy set
			Log.printLine(CloudSim.clock() + " Submitting job " + job.getID() + " to VM " + job.getVM().getId());
			CloudSim.send(engine.getId(), vm.getId(), 0.0, JOB_SUBMIT, job);	
		}
	}
	
	// decide what to do with the job from a new dag
	private boolean admitDAG(DAGJob dj, WorkflowEngine engine) {
			
		double costEstimate = estimateCost(dj, engine);	
		double budgetRemaining = estimateBudgetRemaining(engine);
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
		double sumRuntime = sumRuntime(dj.getDAG());
		double vmPrice = vmPrice(engine);
		return vmPrice*sumRuntime/3600.0;
	}

	/**
	 * Estimate budget remaining, including unused $ and running VMs
	 * TODO: compute budget consumed/remaining by already admitted workflows
	 * @param dj
	 * @param engine
	 * @return
	 */
	private double estimateBudgetRemaining(WorkflowEngine engine) {

		// remaining budget for starting new vms 
		double rn = engine.getBudget()-engine.getCost();

		// compute remaining (not consumed) budget of currently running VMs
		double rc = 0.0;
		
		Set<VM> vms = new HashSet<VM>();
		vms.addAll(engine.getFreeVMs());
		vms.addAll(engine.getBusyVMs());
		
		for (VM vm : vms) {
			rc += vm.getCost() - vm.getRuntime()*vm.getPrice()/3600.0;
		}
		
		// compute remaining runtime of admitted workflows
		double ra = 0.0;
		
		for (DAGJob admittedDJ : admittedDAGs) {
			if (!admittedDJ.isFinished()) {
				ra+=computeRemainingCost(admittedDJ, engine);
			}
		}
		
		// we add this for safety in order not to underestimate our budget
		double safetyMargin = 0.1;
		
		return rn+rc-ra-safetyMargin;
	}

	/** Estimate remaining cost = total remaining time of incomplete tasks * price
	 * 
	 * @param admittedDJ
	 * @param engine
	 * @return
	 */
	private double computeRemainingCost(DAGJob admittedDJ, WorkflowEngine engine) {
		double cost = 0.0;
		DAG dag = admittedDJ.getDAG();
		for (String taskName : dag.getTasks()) {
			Task task = dag.getTask(taskName);
			if (!admittedDJ.isComplete(task)) cost+=task.size*vmPrice(engine);
		}
		return cost/3600.0;
	}
	
	/** 
	 * @return the price of VM hour, assuming that all the vms are homogeneous
	 */
	private double vmPrice(WorkflowEngine engine) {
		double vmPrice = 0;
		if (!engine.getAvailableVMs().isEmpty()) vmPrice = engine.getAvailableVMs().get(0).getPrice();
		return vmPrice;
	}
	
	/**
	 * @return The total runtime of all tasks in the workflow
	 */
	public double sumRuntime(DAG dag) {
        double sum = 0.0;
        for (String taskName : dag.getTasks()) {
            sum += dag.getTask(taskName).size;
        }
        return sum;
    }
}
