package cws.core.algorithms;

import java.util.LinkedList;
import java.util.List;

import cws.core.dag.DAG;

public abstract class Algorithm {
    private double budget;
    private double deadline;
    private List<DAG> dags;
    
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
    
    abstract public void simulate(String logname);
    
    abstract public double getActualCost();
    
    abstract public double getActualFinishTime();
    
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
        double score = 0.0;
        for (int priority : completedDAGPriorities()) {
            score += 1.0 / Math.pow(2.0, priority);
        }
        return score;
    }
    
    /** score = sum[ 1 / priority ] */
    public double getLinearScore() {
        double score = 0.0;
        for (int priority : completedDAGPriorities()) {
            score += 1.0 / (priority+1);
        }
        return score;
    }
}
