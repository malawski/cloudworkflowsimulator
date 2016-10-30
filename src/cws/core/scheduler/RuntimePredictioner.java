package cws.core.scheduler;

import cws.core.VM;
import cws.core.core.VMType;
import cws.core.dag.DAG;
import cws.core.dag.Task;

/**
 * Service that predicts runtimes of tasks and dags. Different implementations may take different signals into account.
 */
public interface RuntimePredictioner {
    /**
     * Returns projected runtime of the given task, based on some assumptions (e.g. whether file transfers are ignored
     * or not).
     * 
     * Should be overridden in pair with the DAG predicting method.
     */
    double getPredictedRuntime(Task task, VM vm, VMType vmType);

    /**
     * Returns projected runtime of the given DAG, based on some assumptions (e.g. whether file transfers are ignored
     * or not).
     * 
     * Should be overridden in pair with the DAG predicting method.
     */
    double getPredictedRuntime(DAG dag, VM vm);
}
