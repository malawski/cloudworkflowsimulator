package cws.core.scheduler;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.Set;

import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.core.CloudSim;

import cws.core.Job;
import cws.core.Scheduler;
import cws.core.VM;
import cws.core.WorkflowEngine;
import cws.core.WorkflowEvent;
import cws.core.dag.Task;

/**
 * This scheduler submits jobs to VMs on FCFS basis.
 * Job is submitted to VM only if VM is idle (no queuing in VMs).
 * @author malawski
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
        
        // XXX: seems unnecessary
        Set<VM> busyVMs = engine.getBusyVMs();

        while (isPossibilityToSchedule(jobs, freeVMs)) {
        	VM vm = getFirst(freeVMs);
        	markVMAsBusy(freeVMs, busyVMs, vm);
        	
            Job job = jobs.poll();
            job.setVM(vm);
            
            List<Job> inputTransferJob = createInputTransferJobs(job, vm);
            List<Job> outputTransferJob = createOutputTransferJobs(job, vm);
            
            for (Job inputJob : inputTransferJob) {
				submitJob(engine, inputJob, vm);
			}
            
            submitJob(engine, job, vm);
            
            for (Job outputJob : outputTransferJob) {
				submitJob(engine, outputJob, vm);
			}
            
        }
    }

	private List<Job> createOutputTransferJobs(Job job, VM vm) {
		List<Job> jobs = new ArrayList<Job>();
		
		Task task = job.getTask();
		// FIXME: no idea why it could be null, parser should be fixed and this check removed
		if(task.getOutputFiles() == null) {
			return Collections.emptyList();
		}
		
				
		for (String file : task.getOutputFiles()) {
			jobs.add(createOutputTransferJob(file, vm, job));
		}
		
		return jobs;
	}

	private Job createOutputTransferJob(String file	, VM vm, Job job) {
		Task dataTask = new Task("output-gs-" + job.getTask().getId() + "-" + file, "data" + job.getTask().getTransformation(), 12.0); 
		
		Job datajob = new Job(dataTask.getSize());
		datajob.setTask(dataTask);
		datajob.setOwner(job.getOwner());
		datajob.setVM(vm);
		return datajob;
	}

	private List<Job> createInputTransferJobs(Job job, VM vm) {
		List<Job> jobs = new ArrayList<Job>();
		
		Task task = job.getTask();
		// FIXME: no idea why it could be null, parser should be fixed and this check removed
		if(task.getInputFiles() == null) {
			return Collections.emptyList();
		}
		
		for (String file : task.getInputFiles()) {
			jobs.add(createInputTransferJob(file, vm, job));
		}
		
		return jobs;
	}

	private Job createInputTransferJob(String file	, VM vm, Job job) {
		Task dataTask = new Task("input-gs-" + job.getTask().getId() + "-" + file, "data" + job.getTask().getTransformation(), 12.0); 
		
		Job datajob = new Job(dataTask.getSize());
		datajob.setTask(dataTask);
		datajob.setOwner(job.getOwner());
		datajob.setVM(vm);
		return datajob;
	}

	private boolean isPossibilityToSchedule(Queue<Job> jobs, Set<VM> freeVMs) {
		return !freeVMs.isEmpty() && !jobs.isEmpty();
	}

	private void submitJob(WorkflowEngine engine, Job job, VM vm) {
		Log.printLine(CloudSim.clock() + " Submitting job " + job.getTask().getId() + " to VM " + job.getVM().getId());
		CloudSim.send(engine.getId(), vm.getId(), 0.0, JOB_SUBMIT, job);
	}

	private void markVMAsBusy(Set<VM> freeVMs, Set<VM> busyVMs, VM vm) {
		freeVMs.remove(vm); // remove VM from free set
		busyVMs.add(vm); // add vm to busy set
	}

	private VM getFirst(Set<VM> freeVMs) {
		return freeVMs.iterator().next();
	}

    @Override
    public void setWorkflowEngine(WorkflowEngine engine) {
        // do nothing
    }
}
