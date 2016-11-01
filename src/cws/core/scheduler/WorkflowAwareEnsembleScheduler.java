package cws.core.scheduler;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

import cws.core.VM;
import cws.core.WorkflowEngine;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.core.VMType;
import cws.core.engine.Environment;
import cws.core.jobs.Job;

/**
 * This scheduler submits workflow ensemble to VMs on FCFS basis.
 * Job is submitted to VM only if VM is idle (no queueing in VMs)
 * and if there are no higher priority jobs in the queue.
 * 
 * @author malawski
 */
public class WorkflowAwareEnsembleScheduler extends EnsembleDynamicScheduler {
    private final WorkflowAdmissioner workflowAdmissioner;

    public WorkflowAwareEnsembleScheduler(CloudSimWrapper cloudsim, Environment environment,
                                          WorkflowAdmissioner workflowAdmissioner, VMType representativeVmType) {
        super(cloudsim, environment, representativeVmType);
        this.workflowAdmissioner = workflowAdmissioner;
    }

    /**
     * Schedule all jobs from the queue to available free VMs.
     * Successfully scheduled jobs are removed from the queue.
     * @param jobs
     * @param engine
     */
    @Override
    protected void scheduleQueue(Queue<Job> jobs, WorkflowEngine engine) {
        List<VM> freeVMs = new ArrayList<VM>(engine.getFreeVMs());
        while (!jobs.isEmpty() && !freeVMs.isEmpty()) {
            Job job = jobs.poll();

            if (workflowAdmissioner.isJobDagAdmitted(job, engine, selectBestVM())) {
                VM vm = freeVMs.remove(freeVMs.size() - 1);
                vm.jobSubmit(job);
            }
        }
    }
}
