package cws.core.algorithms;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;

import cws.core.VM;
import cws.core.VMListener;
import cws.core.cloudsim.CWSSimEntity;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.dag.DAG;
import cws.core.dag.DAGJob;
import cws.core.dag.DAGJobListener;
import cws.core.engine.Environment;
import cws.core.jobs.Job;
import cws.core.jobs.Job.Result;
import cws.core.jobs.JobListener;

public class AlgorithmStatistics extends CWSSimEntity implements DAGJobListener, VMListener, JobListener {
    private final List<DAG> allDags;
    private final double budget;
    private final double deadline;
    private final Environment environment;

    public AlgorithmStatistics(List<DAG> allDags, double budget, double deadline, CloudSimWrapper cloudsim,
            Environment environment) {
        super("AlgorithmStatistics", cloudsim);
        this.allDags = allDags;
        this.budget = budget;
        this.deadline = deadline;
        this.environment = environment;
    }

    private double lastJobFinishTime = 0.0;
    private double lastVmFinishTime = 0.0;
    private double lastDagFinishTime = 0.0;

    /**
     * DAGs that finished within budget and deadline constraints.
     */
    private List<DAG> finishedDagsWithinBudgetAndDeadline = new ArrayList<DAG>();

    /**
     * All VMs that were ever created in the simulation.
     */
    private List<VM> allVMs = new ArrayList<VM>();

    @Override
    public void shutdownEntity() {
        getCloudsim().log("Actual cost: " + this.getCost());
        getCloudsim().log("Last DAG finished at: " + this.getLastDagFinishTime());
        getCloudsim().log("Last time VM terminated at: " + this.getLastVMFinishTime());
        getCloudsim().log("Last time Job terminated at: " + this.getLastJobFinishTime());
    }

    public List<Integer> getFinishedDAGPriorities() {
        List<Integer> priorities = new LinkedList<Integer>();
        for (DAG dag : getFinishedDags()) {
            int index = allDags.indexOf(dag);
            int priority = index;
            priorities.add(priority);
        }
        return priorities;
    }

    public String getFinishedDAGPriorityString() {
        StringBuilder b = new StringBuilder("[");
        boolean first = true;
        for (int priority : getFinishedDAGPriorities()) {
            if (!first) {
                b.append(", ");
            }
            b.append(priority);
            first = false;
        }
        b.append("]");
        return b.toString();
    }

    /** score = sum[ 1 / 2^priority ] */
    public double getExponentialScore() {
        BigDecimal one = BigDecimal.ONE;
        BigDecimal two = new BigDecimal(2.0);

        BigDecimal score = new BigDecimal(0.0);
        for (int priority : getFinishedDAGPriorities()) {
            BigDecimal divisor = two.pow(priority);
            BigDecimal increment = one.divide(divisor);
            score = score.add(increment);
        }
        return score.doubleValue();
    }

    /** score = sum[ 1 / priority ] */
    public double getLinearScore() {
        double score = 0.0;
        for (int priority : getFinishedDAGPriorities()) {
            score += 1.0 / (priority + 1);
        }
        return score;
    }

    public String getScoreBitString() {
        HashSet<Integer> priorities = new HashSet<Integer>(getFinishedDAGPriorities());

        int ensembleSize = allDags.size();

        StringBuilder b = new StringBuilder();

        for (int p = 0; p < ensembleSize; p++) {
            if (priorities.contains(p)) {
                b.append("1");
            } else {
                b.append("0");
            }
        }
        return b.toString();
    }

    /**
     * Returns the cost of all VMs that were ever created till now.
     */
    public double getCost() {
        return environment.getPricingManager().getAllVMsCost(allVMs);
    };

    public double getLastDagFinishTime() {
        return lastDagFinishTime;
    };

    public double getLastJobFinishTime() {
        return lastJobFinishTime;
    };

    public double getLastVMFinishTime() {
        return lastVmFinishTime;
    };

    public List<DAG> getFinishedDags() {
        return finishedDagsWithinBudgetAndDeadline;
    };

    @Override
    public void jobFinished(Job job) {
        if (job.getResult() == Result.SUCCESS) {
            lastJobFinishTime = Math.max(lastJobFinishTime, job.getFinishTime());
        }
    }

    @Override
    public void vmLaunched(VM vm) {
        this.allVMs.add(vm);
    }

    @Override
    public void vmTerminated(VM vm) {
        lastVmFinishTime = Math.max(lastVmFinishTime, getCloudsim().clock());
    }

    @Override
    public void dagStarted(DAGJob dagJob) {
    }

    @Override
    public void dagFinished(DAGJob dagJob) {
        lastDagFinishTime = Math.max(lastDagFinishTime, getCloudsim().clock());
        if (withinBudgetAndDeadline()) {
            finishedDagsWithinBudgetAndDeadline.add(dagJob.getDAG());
        }
    }

    /**
     * Returns true when current time of simulation is within budget and deadline constraints.
     */
    private boolean withinBudgetAndDeadline() {
        return getCost() <= budget && getCloudsim().clock() <= deadline;
    }

    /**
     * Returns total time of all VMs spent on file transfers. The assumption is that VMs are 1-core.
     */
    public double getTimeSpentOnTransfers() {
        double time = 0;
        for (VM vm : allVMs) {
            time += vm.getTimeSpentOnTransfers();
        }
        return time;
    }

    /**
     * Returns total time of all VMs spent on computations. The assumption is that VMs are 1-core.
     */
    public double getTimeSpentOnComputations() {
        double time = 0;
        for (VM vm : allVMs) {
            time += vm.getTimeSpentOnComputations();
        }
        return time;
    }

    @Override
    public void jobReleased(Job job) {
    }

    @Override
    public void jobSubmitted(Job job) {
    }

    @Override
    public void jobStarted(Job job) {
    }
}
