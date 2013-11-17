package cws.core;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;

import cws.core.cloudsim.CWSSimEntity;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.dag.DAG;
import cws.core.dag.DAGJob;
import cws.core.dag.DAGJobListener;
import cws.core.jobs.Job;
import cws.core.jobs.Job.Result;
import cws.core.jobs.JobListener;

public class AlgorithmStatistics extends CWSSimEntity implements DAGJobListener, VMListener, JobListener {
    private List<DAG> allDags;

    public AlgorithmStatistics(List<DAG> allDags, CloudSimWrapper cloudsim) {
        super("AlgorithmStatistics", cloudsim);
        this.allDags = allDags;
    }

    private double actualJobFinishTime = 0.0;
    private double actualVmFinishTime = 0.0;
    private double actualDagFinishTime = 0.0;
    private List<DAG> finishedDags = new ArrayList<DAG>();
    private double cost = 0.0;

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

    public double getActualCost() {
        return cost;
    };

    public double getActualDagFinishTime() {
        return actualDagFinishTime;
    };

    public double getActualJobFinishTime() {
        return actualJobFinishTime;
    };

    public double getActualVMFinishTime() {
        return actualVmFinishTime;
    };

    public List<DAG> getFinishedDags() {
        return finishedDags;
    };

    @Override
    public void jobReleased(Job job) {
    }

    @Override
    public void jobSubmitted(Job job) {
    }

    @Override
    public void jobStarted(Job job) {
    }

    @Override
    public void jobFinished(Job job) {
        if (job.getResult() == Result.SUCCESS) {
            actualJobFinishTime = Math.max(actualJobFinishTime, job.getFinishTime());
        }
    }

    @Override
    public void vmLaunched(VM vm) {
    }

    @Override
    public void vmTerminated(VM vm) {
        cost += vm.getCost();
        actualVmFinishTime = Math.max(actualVmFinishTime, getCloudsim().clock());
    }

    @Override
    public void dagStarted(DAGJob dagJob) {
    }

    @Override
    public void dagFinished(DAGJob dagJob) {
        actualDagFinishTime = Math.max(actualDagFinishTime, getCloudsim().clock());
        finishedDags.add(dagJob.getDAG());
    }
}
