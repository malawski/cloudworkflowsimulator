package cws.core.scheduler;

import java.util.Iterator;
import java.util.List;
import java.util.Queue;

import cws.core.Scheduler;
import cws.core.VM;
import cws.core.WorkflowEngine;
import cws.core.jobs.Job;

/**
 * This scheduler submits jobs to VMs on FCFS basis.
 * The ready jobs are inserted into VM queues for execution.
 * @author malawski
 */
public class DAGSchedulerFCFS implements Scheduler {
    @Override
    public void scheduleJobs(WorkflowEngine engine) {
        Queue<Job> jobs = engine.getQueuedJobs();
        List<VM> vms = engine.getAvailableVMs();

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
                vm.jobSubmit(job);
            }
        }
    }
}
