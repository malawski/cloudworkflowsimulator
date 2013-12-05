package cws.core;

import java.util.Collection;
import java.util.LinkedList;

import cws.core.cloudsim.CWSSimEntity;
import cws.core.cloudsim.CWSSimEvent;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.dag.DAG;
import cws.core.dag.DAGJob;
import cws.core.dag.DAGJobListener;

/**
 * This class manages a collection of DAGs and submits them to a WorkflowEngine
 * for execution in priority order according to a scheduling algorithm.
 * 
 * @author Gideon Juve <juve@usc.edu>
 */
public class EnsembleManager extends CWSSimEntity {
    /** List of all DAGs remaining to be executed */
    private LinkedList<DAGJob> dags = new LinkedList<DAGJob>();

    /** DAG listeners */
    private LinkedList<DAGJobListener> listeners;

    /** Workflow engine that will receive DAGs for execution */
    private WorkflowEngine engine = null;

    public EnsembleManager(Collection<DAG> dags, WorkflowEngine engine, CloudSimWrapper cloudsim) {
        super("EnsembleManager", cloudsim);
        this.engine = engine;
        this.dags = new LinkedList<DAGJob>();
        this.listeners = new LinkedList<DAGJobListener>();
        prioritizeDAGs(dags);
    }

    public EnsembleManager(WorkflowEngine engine, CloudSimWrapper cloudsim) {
        this(null, engine, cloudsim);
    }

    private void prioritizeDAGs(Collection<DAG> dags) {
        if (dags == null)
            return;

        // For now just add them in whatever order they come in
        int priority = 0;
        for (DAG d : dags) {
            DAGJob dj = new DAGJob(d, getId());
            dj.setPriority(priority++);
            this.dags.add(dj);
        }
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
        while (!dags.isEmpty()) {
            submitDAG(dags.pop());
        }
    }

    @Override
    public void processEvent(CWSSimEvent ev) {
        switch (ev.getTag()) {
        case WorkflowEvent.DAG_STARTED:
            dagStarted((DAGJob) ev.getData());
            break;
        case WorkflowEvent.DAG_FINISHED:
            dagFinished((DAGJob) ev.getData());
            break;
        default:
            throw new RuntimeException("Unknown event: " + ev);
        }
    }

    @Override
    public void shutdownEntity() {
        // Do nothing
    }

    public void submitDAG(DAGJob dagJob) {
        // Submit the dag to the workflow engine
        sendNow(engine.getId(), WorkflowEvent.DAG_SUBMIT, dagJob);
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
        // Remove the DAG
        dags.remove(dag);
    }
}
