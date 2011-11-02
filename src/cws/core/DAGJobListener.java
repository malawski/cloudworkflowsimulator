package cws.core;

/**
 * An interface for parties interested in receiving notifications about DAG
 * execution.
 *
 * @author Gideon Juve <juve@usc.edu>
 */
public interface DAGJobListener {
    /** A DAG began executing */
    public void dagStarted(DAGJob dagJob);
    
    /** A DAG finished executing */
    public void dagFinished(DAGJob dagJob);
}
