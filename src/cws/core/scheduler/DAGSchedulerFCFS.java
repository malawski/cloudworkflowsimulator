package cws.core.scheduler;

import java.util.Iterator;
import java.util.List;
import java.util.Queue;

import cws.core.Scheduler;
import cws.core.VM;
import cws.core.WorkflowEngine;
import cws.core.WorkflowEvent;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.jobs.Job;
import cws.core.storage.StorageManager;

/**
 * This scheduler submits jobs to VMs on FCFS basis.
 * The ready jobs are inserted into VM queues for execution.
 * @author malawski
 */
public class DAGSchedulerFCFS implements Scheduler {

    private List<VM> vms;

    private CloudSimWrapper cloudsim;

    public DAGSchedulerFCFS(CloudSimWrapper cloudsim) {
        this.cloudsim = cloudsim;
    }

    @Override
    public void scheduleJobs(WorkflowEngine engine) {
        Queue<Job> jobs = engine.getQueuedJobs();
        vms = engine.getAvailableVMs();

        // if there is nothing to do, just return
        if (vms.isEmpty())
            return;
        if (jobs.isEmpty())
            return;

        Iterator<VM> vmIt = vms.iterator();
        while (!jobs.isEmpty() && vmIt.hasNext()) {
            VM vm = vmIt.next();
            if (vm.getQueueLength() == 0) {
                Job job = jobs.poll(); // retrieve and remove job from ready set
                job.setVM(vm);
                cloudsim.log(" Submitting job " + job.getID() + " to VM " + job.getVM().getId());
                cloudsim.send(engine.getId(), vm.getId(), 0.0, WorkflowEvent.JOB_SUBMIT, job);
            }
        }
    }

    @Override
    public void setStorageManager(StorageManager storageManager) {
        // do nothing, we don't need storage manager here
    }
}
