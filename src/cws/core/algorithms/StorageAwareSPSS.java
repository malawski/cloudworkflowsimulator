package cws.core.algorithms;

import java.util.HashMap;
import java.util.List;

import cws.core.cloudsim.CloudSimWrapper;
import cws.core.dag.DAG;
import cws.core.dag.Task;
import cws.core.dag.algorithms.CriticalPath;
import cws.core.dag.algorithms.StorageAwareCriticalPath;
import cws.core.dag.algorithms.TopologicalOrder;
import cws.core.engine.Environment;

/**
 * Storage aware version of the SPSS algorithm.
 */
public class StorageAwareSPSS extends SPSS {
    public StorageAwareSPSS(double budget, double deadline, List<DAG> dags, double alpha,
            AlgorithmStatistics ensembleStatistics, Environment environment, CloudSimWrapper cloudsim) {
        super(budget, deadline, dags, alpha, ensembleStatistics, environment, cloudsim);
    }

    @Override
    protected CriticalPath newCriticalPath(TopologicalOrder order, HashMap<Task, Double> runtimes) {
        return new StorageAwareCriticalPath(order, runtimes, getEnvironment());
    }

    @Override
    protected double getPredictedTaskRuntime(Task task) {
        return getEnvironment().getComputationPredictedRuntime(task)
                + getEnvironment().getTransfersPredictedRuntime(task);
    }
}
