package cws.core.algorithms;

import java.math.BigDecimal;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;

import cws.core.dag.DAG;

public abstract class Algorithm {
    private double budget;
    private double deadline;
    private List<DAG> dags;
    private boolean generateLog = false;
    
    public Algorithm(double budget, double deadline, List<DAG> dags) {
        this.budget = budget;
        this.deadline = deadline;
        this.dags = dags;
    }
    
    public List<DAG> getDAGs() {
        return dags;
    }
    
    public double getBudget() {
        return budget;
    }
    
    public double getDeadline() {
        return deadline;
    }
    
    public String getName() {
        return this.getClass().getSimpleName();
    }
    
    public boolean shouldGenerateLog() {
        return this.generateLog;
    }
    
    public void setGenerateLog(boolean generateLog) {
        this.generateLog = generateLog;
    }
    
    abstract public void simulate(String logname);
    
    abstract public double getActualCost();
    
    /**
     * @return Finish time of the last completed dag
     */
    abstract public double getActualDagFinishTime();
    
    /**
     * @return Finish time of the last successfully completed job
     */
    abstract public double getActualJobFinishTime();
    
    /**
     * @return Termination time of the last VM
     */
    abstract public double getActualVMFinishTime();
    
    abstract public long getSimulationWallTime();
    
    abstract public long getPlanningnWallTime();
    
    abstract public List<DAG> getCompletedDAGs();
    
    public int numCompletedDAGs() {
        return getCompletedDAGs().size();
    }
    
    public List<Integer> completedDAGPriorities() {
        List<DAG> dags = getDAGs();
        List<Integer> priorities = new LinkedList<Integer>();
        for (DAG dag : getCompletedDAGs()) {
            int index = dags.indexOf(dag);
            int priority = index;
            priorities.add(priority);
        }
        return priorities;
    }
    
    public String completedDAGPriorityString() {
        StringBuilder b = new StringBuilder("[");
        boolean first = true;
        for (int priority : completedDAGPriorities()) {
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
        for (int priority : completedDAGPriorities()) {
            BigDecimal divisor = two.pow(priority);
            BigDecimal increment = one.divide(divisor);
            score = score.add(increment);
        }
        return score.doubleValue();
    }
    
    /** score = sum[ 1 / priority ] */
    public double getLinearScore() {
        double score = 0.0;
        for (int priority : completedDAGPriorities()) {
            score += 1.0 / (priority+1);
        }
        return score;
    }
    
    public String getScoreBitString() {
        HashSet<Integer> priorities = new HashSet<Integer>(
                completedDAGPriorities());
        
        int ensembleSize = getDAGs().size();
        
        StringBuilder b = new StringBuilder();
        
        for (int p=0; p<ensembleSize; p++) {
            if (priorities.contains(p)) {
                b.append("1");
            } else {
                b.append("0");
            }
        }
        
        return b.toString();
    }
}
