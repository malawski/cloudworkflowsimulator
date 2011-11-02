package cws.core;

import cws.core.dag.DAG;

/**
 * This class records information about the execution of a DAG.
 *
 * @author Gideon Juve <juve@usc.edu>
 */
public class DAGJob {
    /** The entity that owns the DAG */
    private int owner;
    
    /** The DAG being executed */
    private DAG dag;
    
    public DAGJob(DAG dag, int owner) {
        this.dag = dag;
        this.owner = owner;
    }
    
    public int getOwner() {
        return owner;
    }
    
    public void setOwner(int owner) {
        this.owner = owner;
    }
    
    public DAG getDAG() {
        return dag;
    }
    
    public void setDAG(DAG dag) {
        this.dag = dag;
    }
}
