package cws.core.scheduler;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;

import cws.core.Scheduler;
import cws.core.VM;
import cws.core.WorkflowEngine;
import cws.core.WorkflowEvent;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.dag.Task;
import cws.core.jobs.Job;

/**
 * This scheduler submits jobs to VMs on FCFS basis.
 * Job is submitted to VM only if VM is idle (no queuing in VMs).
 * @author malawski
 */
public class DAGDynamicScheduler implements Scheduler {

    private CloudSimWrapper cloudsim;

    public DAGDynamicScheduler(CloudSimWrapper cloudsim) {
        this.cloudsim = cloudsim;
    }

    @Override
    public void setWorkflowEngine(WorkflowEngine engine) {
        // do nothing
    }

    @Override
    public void scheduleJobs(WorkflowEngine engine) {
        // use the queued (released) jobs from the workflow engine
        Queue<Job> jobs = engine.getQueuedJobs();

        scheduleQueue(jobs, engine);
    }

    protected CloudSimWrapper getCloudSim() {
        return cloudsim;
    }

    /**
     * Schedule all jobs from the queue to available free VMs.
     * Successfully scheduled jobs are removed from the queue.
     * @param jobs
     * @param engine
     */
    protected void scheduleQueue(Queue<Job> jobs, WorkflowEngine engine) {
        // FIXME(_mequrel_): copying references because when we remove it from list, garbage collector removes VM...
        // imho it shouldnt working like that
        Set<VM> freeVMs = new HashSet<VM>(engine.getFreeVMs());

        while (canBeScheduled(jobs, freeVMs)) {
            Job job = jobs.poll();
            scheduleJob(job, freeVMs, engine);
        }
    }

    protected void scheduleJob(Job job, Set<VM> freeVMs, WorkflowEngine engine) {
        VM vm = getFirst(freeVMs);
        markVMAsBusy(freeVMs, vm);

        job.setVM(vm);

        List<Job> inputTransferJob = createInputTransferJobs(job, vm);
        List<Job> outputTransferJob = createOutputTransferJobs(job, vm);

        for (Job inputJob : inputTransferJob) {
            sendJobToVM(engine, vm, inputJob);
        }

        sendJobToVM(engine, vm, job);

        for (Job outputJob : outputTransferJob) {
            sendJobToVM(engine, vm, outputJob);
        }
    }

    private void sendJobToVM(WorkflowEngine engine, VM vm, Job job) {
        cloudsim.send(engine.getId(), vm.getId(), 0.0, WorkflowEvent.JOB_SUBMIT, job);
        cloudsim.log("Submitting job " + job.getTask().getId() + " to VM " + job.getVM().getId());
    }

    private List<Job> createOutputTransferJobs(Job job, VM vm) {
        List<Job> jobs = new ArrayList<Job>();

        Task task = job.getTask();
        // FIXME(_mequrel_): no idea why it could be null, parser should be fixed and this check removed
        if (task.getOutputFiles() == null) {
            return Collections.emptyList();
        }

        for (String file : task.getOutputFiles()) {
            jobs.add(createOutputTransferJob(file, vm, job));
        }

        return jobs;
    }

    private Job createOutputTransferJob(String file, VM vm, Job job) {
        Task dataTask = new Task("output-gs-" + job.getTask().getId() + "-" + file, "data"
                + job.getTask().getTransformation(), 12.0);

        Job datajob = new Job(cloudsim);
        datajob.setTask(dataTask);
        datajob.setOwner(job.getOwner());
        datajob.setVM(vm);
        return datajob;
    }

    private List<Job> createInputTransferJobs(Job job, VM vm) {
        List<Job> jobs = new ArrayList<Job>();

        Task task = job.getTask();
        // FIXME(_mequrel_): no idea why it could be null, parser should be fixed and this check removed
        if (task.getInputFiles() == null) {
            return Collections.emptyList();
        }

        for (String file : task.getInputFiles()) {
            jobs.add(createInputTransferJob(file, vm, job));
        }

        return jobs;
    }

    private Job createInputTransferJob(String file, VM vm, Job job) {
        Task dataTask = new Task("input-gs-" + job.getTask().getId() + "-" + file, "data"
                + job.getTask().getTransformation(), 12.0);

        Job datajob = new Job(cloudsim);
        datajob.setTask(dataTask);
        datajob.setOwner(job.getOwner());
        datajob.setVM(vm);
        return datajob;
    }

    private boolean canBeScheduled(Queue<Job> jobs, Set<VM> freeVMs) {
        return !freeVMs.isEmpty() && !jobs.isEmpty();
    }

    private void markVMAsBusy(Set<VM> freeVMs, VM vm) {
        freeVMs.remove(vm);
    }

    private VM getFirst(Set<VM> freeVMs) {
        return freeVMs.iterator().next();
    }
}
