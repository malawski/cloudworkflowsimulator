package cws.core.scheduler;

import java.util.HashSet;
import java.util.Set;

import cws.core.VM;
import cws.core.WorkflowEngine;
import cws.core.cloudsim.CWSSimEntity;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.core.VMType;
import cws.core.dag.DAG;
import cws.core.dag.DAGJob;
import cws.core.dag.Task;
import cws.core.engine.Environment;
import cws.core.jobs.Job;
import cws.core.pricing.PricingManager;

/**
 * WorkflowAdmissioner that decides on workflow admission based on its runtime predictions.
 */
public final class RuntimeWorkflowAdmissioner extends CWSSimEntity implements WorkflowAdmissioner {
    private final Environment environment;
    private final RuntimePredictioner runtimePredictioner;
    private final Set<DAGJob> admittedDAGs = new HashSet<DAGJob>();
    private final Set<DAGJob> rejectedDAGs = new HashSet<DAGJob>();
    private final VMType selectedVmType;

    public RuntimeWorkflowAdmissioner(CloudSimWrapper cloudsim, RuntimePredictioner runtimePredictioner,
                                      Environment environment, VMType selectedVmType) {
        super("WorkflowAdmissioner", cloudsim);
        this.environment = environment;
        this.selectedVmType = selectedVmType;
        this.runtimePredictioner = runtimePredictioner;
    }

    @Override
    public final boolean isJobDagAdmitted(Job job, WorkflowEngine engine, VM vm) {
        DAGJob dj = job.getDAGJob();

        if (jobHasBeenAlreadyAdmitted(dj)) {
            return true;
        } else if (jobHasBeenAlreadyRejected(dj)) {
            return false;
        } else {
            boolean isAdmittable = isJobAdmittable(dj, engine, vm);
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
    private boolean isJobAdmittable(DAGJob dj, WorkflowEngine engine, VM vm) {
        double costEstimate = estimateCost(dj, vm);
        double budgetRemaining = estimateBudgetRemaining(engine, vm);
        getCloudsim().log(" Cost estimate: " + costEstimate + " Budget remaining: " + budgetRemaining);
        return costEstimate < budgetRemaining; // TODO(bryk): Add critical path here.
    }

    /**
     * Estimate cost of this workflow
     */
    private double estimateCost(DAGJob dj, VM vm) {
        double runtimeSum = runtimePredictioner.getPredictedRuntime(dj.getDAG(), vm);
        return costForRuntimeSum(runtimeSum, vm);
    }

    /**
     * Estimate budget remaining, including unused $ and running VMs
     */
    private double estimateBudgetRemaining(WorkflowEngine engine, VM bestVM) {
        // remaining budget for starting new vms
        double rn = engine.getBudget() - engine.getCost();
        if (rn < 0)
            rn = 0;

        // compute remaining (not consumed) budget of currently running VMs
        double rc = 0.0;

        Set<VM> vms = new HashSet<VM>();
        vms.addAll(engine.getFreeVMs());
        vms.addAll(engine.getBusyVMs());

        PricingManager pricingManager = environment.getPricingManager();
        for (VM vm : vms) {
            rc += pricingManager.getRuntimeVMCost(vm) - pricingManager.getAlreadyPaidCost(vm);
        }

        // compute remaining runtime of admitted workflows
        double ra = 0.0;

        for (DAGJob admittedDJ : admittedDAGs) {
            if (!admittedDJ.isFinished()) {
                ra += computeRemainingCost(admittedDJ, bestVM);
            }
        }

        // we add this for safety in order not to underestimate our budget
        double safetyMargin = 0.1;

        getCloudsim().log(" Budget for new VMs: " + rn + " Budget on running VMs: " + rc
                + " Remaining budget of admitted workflows: " + ra);

        return rn + rc - ra - safetyMargin;
    }

    /**
     * Estimate remaining cost = total remaining time of incomplete tasks * price
     * 
     * @param admittedDJ
     * @return
     */
    private double computeRemainingCost(DAGJob admittedDJ, VM vm) {
        double runtimeSum = 0.0;
        DAG dag = admittedDJ.getDAG();
        for (String taskName : dag.getTasks()) {
            Task task = dag.getTaskById(taskName);
            if (!admittedDJ.isComplete(task)) {
                runtimeSum += runtimePredictioner.getPredictedRuntime(task, null, getSelectedVmType());
            }
        }
        return costForRuntimeSum(runtimeSum, vm);
    }

    private double costForRuntimeSum(final double runtime, VM vm) {
        final double cost = environment.getPricingManager().getVMCostFor(vm.getVmType(), runtime);
        final int cores = vm.getVmType().getCores();
        return  cost / cores;
    }

    public VMType getSelectedVmType() {
        return selectedVmType;
    }
}
