package cws.core.scheduler;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;

import cws.core.VM;
import cws.core.WorkflowEngine;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.dag.DAG;
import cws.core.dag.DAGJob;
import cws.core.dag.Task;
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
    public WorkflowAwareEnsembleScheduler(CloudSimWrapper cloudsim, Environment environment) {
        super(cloudsim, environment);
    }

    private final Set<DAGJob> admittedDAGs = new HashSet<DAGJob>();
    private final Set<DAGJob> rejectedDAGs = new HashSet<DAGJob>();

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

            if (isJobDagAdmitted(job, engine)) {
                VM vm = freeVMs.remove(freeVMs.size() - 1);
                vm.jobSubmit(job);
            }
        }
    }

    protected final boolean isJobDagAdmitted(Job job, WorkflowEngine engine) {
        DAGJob dj = job.getDAGJob();

        if (jobHasBeenAlreadyAdmitted(dj)) {
            return true;
        } else if (jobHasBeenAlreadyRejected(dj)) {
            return false;
        } else {
            boolean isAdmittable = isJobAdmittable(dj, engine);
            rememberAdmitionOrRejection(dj, isAdmittable);
            return isAdmittable;
        }

    }

    private void rememberAdmitionOrRejection(DAGJob dj, boolean isAdmittable) {
        if (isAdmittable) {
            admittedDAGs.add(dj);
        } else {
            rejectedDAGs.add(dj);
        }
    }

    protected boolean jobHasBeenAlreadyRejected(DAGJob dj) {
        return rejectedDAGs.contains(dj);
    }

    protected boolean jobHasBeenAlreadyAdmitted(DAGJob dj) {
        return admittedDAGs.contains(dj);
    }

    // decide what to do with the job from a new dag
    private boolean isJobAdmittable(DAGJob dj, WorkflowEngine engine) {
        double costEstimate = estimateCost(dj);
        double budgetRemaining = estimateBudgetRemaining(engine);
        getCloudsim().log(" Cost estimate: " + costEstimate + " Budget remaining: " + budgetRemaining);
        return costEstimate < budgetRemaining; // TODO(bryk): Add critical path here.
    }

    /**
     * Estimate cost of this workflow
     */
    private double estimateCost(DAGJob dj) {
        double sumRuntime = getPredictedRuntime(dj.getDAG());
        double vmPrice = environment.getSingleVMPrice();
        return vmPrice * sumRuntime / environment.getBillingTimeInSeconds();
    }

    /**
     * Estimate budget remaining, including unused $ and running VMs
     */
    private double estimateBudgetRemaining(WorkflowEngine engine) {
        // remaining budget for starting new vms
        double rn = engine.getBudget() - engine.getCost();
        if (rn < 0)
            rn = 0;

        // compute remaining (not consumed) budget of currently running VMs
        double rc = 0.0;

        Set<VM> vms = new HashSet<VM>();
        vms.addAll(engine.getFreeVMs());
        vms.addAll(engine.getBusyVMs());

        for (VM vm : vms) {
            rc += vm.getCost() - vm.getRuntime() * vm.getVmType().getPriceForBillingUnit()
                    / environment.getBillingTimeInSeconds();
        }

        // compute remaining runtime of admitted workflows
        double ra = 0.0;

        for (DAGJob admittedDJ : admittedDAGs) {
            if (!admittedDJ.isFinished()) {
                ra += computeRemainingCost(admittedDJ, engine);
            }
        }

        // we add this for safety in order not to underestimate our budget
        double safetyMargin = 0.1;

        getCloudsim().log(
                " Budget for new VMs: " + rn + " Budget on running VMs: " + rc
                        + " Remaining budget of admitted workflows: " + ra);

        return rn + rc - ra - safetyMargin;
    }

    /**
     * Estimate remaining cost = total remaining time of incomplete tasks * price
     * 
     * @param admittedDJ
     * @param engine
     * @return
     */
    private double computeRemainingCost(DAGJob admittedDJ, WorkflowEngine engine) {
        double cost = 0.0;
        DAG dag = admittedDJ.getDAG();
        for (String taskName : dag.getTasks()) {
            Task task = dag.getTaskById(taskName);
            if (!admittedDJ.isComplete(task)) {
                cost += getPredictedRuntime(task, null) * environment.getSingleVMPrice();
            }
        }
        return cost / environment.getBillingTimeInSeconds();
    }

    /**
     * Returns projected runtime of the given task, based on some assumptions (e.g. whether file transfers are ignored
     * or not).
     * 
     * Should be overridden in pair with the DAG predicting method.
     */
    protected double getPredictedRuntime(Task task, VM vm) {
        return environment.getComputationPredictedRuntime(task);
    }

    /**
     * Returns projected runtime of the given DAG, based on some assumptions (e.g. whether file transfers are ignored
     * or not).
     * 
     * Should be overridden in pair with the DAG predicting method.
     */
    protected double getPredictedRuntime(DAG dag) {
        return environment.getComputationPredictedRuntime(dag);
    }
}
