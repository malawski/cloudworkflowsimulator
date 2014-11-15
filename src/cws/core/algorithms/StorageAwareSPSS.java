package cws.core.algorithms;

import java.util.List;

import cws.core.cloudsim.CloudSimWrapper;
import cws.core.dag.DAG;
import cws.core.dag.Task;
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
    protected double getPredictedTaskRuntime(Task task) {
        return getEnvironment().getComputationPredictedRuntime(task)
                + getEnvironment().getTransfersPredictedRuntime(task);
    }
}
