package cws.core;

import java.util.Collection;
import java.util.LinkedList;

import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.SimEntity;
import org.cloudbus.cloudsim.core.SimEvent;

import cws.core.dag.DAG;

/**
 * This class manages a collection of DAGs and submits them to a WorkflowEngine
 * for execution in priority order according to a scheduling algorithm.
 *
 * @author Gideon Juve <juve@usc.edu>
 */
public class EnsembleManager extends SimEntity implements WorkflowEvent {
    /** List of all DAGs remaining to be executed */
    private LinkedList<DAGJob> dags = new LinkedList<DAGJob>();
    
    /** DAG listeners */
    private LinkedList<DAGJobListener> listeners;
    
    /** Workflow engine that will receive DAGs for execution */
    private WorkflowEngine engine = null;
    
    public EnsembleManager(Collection<DAG> dags, WorkflowEngine engine) {
        super("EnsembleManager");
        this.engine = engine;
        this.dags = new LinkedList<DAGJob>();
        this.listeners = new LinkedList<DAGJobListener>();
        prioritizeDAGs(dags);
        CloudSim.addEntity(this);
    }
    
    public void addDAGJobListener(DAGJobListener l) {
        this.listeners.add(l);
    }
    
    public void removeDAGJobListener(DAGJobListener l) {
        this.listeners.remove(l);
    }
    
    @Override
    public void startEntity() {
        // Submit all DAGs
    	while(!dags.isEmpty()) {
    		submitDAG(dags.pop());
    	}
    }
    
    @Override
    public void processEvent(SimEvent ev) {
        switch (ev.getTag()) {
            case DAG_STARTED:
                dagStarted((DAGJob)ev.getData());
                break;
            case DAG_FINISHED:
                dagFinished((DAGJob)ev.getData());
                break;
            default:
                throw new RuntimeException("Unknown event: "+ev);
        }
    }

    @Override
    public void shutdownEntity() {
        // Do nothing
    }
    
    private void prioritizeDAGs(Collection<DAG> dags) {
        // For now just add them in whatever order they come in
    	int priority = 0;
        for (DAG d : dags) {
        	DAGJob dj = new DAGJob(d, getId());
        	dj.setPriority(priority++);
            this.dags.add(dj);
        }
    }
    
    private void submitDAG(DAGJob dagJob) {
        // Submit the dag to the workflow engine
        sendNow(engine.getId(), DAG_SUBMIT, dagJob);
    }
    
    private void dagStarted(DAGJob dag) {
        // Notify all listeners
        for (DAGJobListener l : listeners) {
            l.dagStarted(dag);
        }
    }
    
    private void dagFinished(DAGJob dag) {
        // Notify all listeners
        for (DAGJobListener l : listeners) {
            l.dagFinished(dag);
        }
        
        // Submit the next dag
        if (dags.size() > 0)
            submitDAG(dags.pop());
    }
}
