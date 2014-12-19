package cws.core.scheduler;

import java.util.HashSet;
import java.util.Set;

import cws.core.VM;
import cws.core.WorkflowEngine;
import cws.core.cloudsim.CWSSimEntity;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.dag.DAG;
import cws.core.dag.DAGJob;
import cws.core.dag.Task;
import cws.core.engine.Environment;
import cws.core.jobs.Job;

/**
 * WorkflowAdmissioner that decides on workflow admission based on its runtime predictions.
 */
public final class RuntimeWorkflowAdmissioner extends CWSSimEntity implements WorkflowAdmissioner {
    private final Environment environment;
    private final RuntimePredictioner runtimePredictioner;
    private final Set<DAGJob> admittedDAGs = new HashSet<DAGJob>();
    private final Set<DAGJob> rejectedDAGs = new HashSet<DAGJob>();

    public RuntimeWorkflowAdmissioner(CloudSimWrapper cloudsim, RuntimePredictioner runtimePredictioner,
            Environment environment) {
        super("WorkflowAdmissioner", cloudsim);
        this.environment = environment;
        this.runtimePredictioner = runtimePredictioner;
    }

    @Override
    public final boolean isJobDagAdmitted(Job job, WorkflowEngine engine) {
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

    private boolean jobHasBeenAlreadyRejected(DAGJob dj) {
        return rejectedDAGs.contains(dj);
    }

    private boolean jobHasBeenAlreadyAdmitted(DAGJob dj) {
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
        double sumRuntime = runtimePredictioner.getPredictedRuntime(dj.getDAG());
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
                cost += runtimePredictioner.getPredictedRuntime(task, null) * environment.getSingleVMPrice();
            }
        }
        return cost / environment.getBillingTimeInSeconds();
    }
}
