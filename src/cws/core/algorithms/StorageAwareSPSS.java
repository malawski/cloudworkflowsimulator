package cws.core.algorithms;

import java.util.HashMap;
import java.util.List;

import cws.core.cloudsim.CloudSimWrapper;
import cws.core.core.VMType;
import cws.core.dag.DAG;
import cws.core.dag.Task;
import cws.core.dag.algorithms.CriticalPath;
import cws.core.dag.algorithms.StorageAwareCriticalPath;
import cws.core.dag.algorithms.TopologicalOrder;
import cws.core.engine.Environment;

/**
 * Storage aware version of the SPSS algorithm.
 * 
 * Storage awareness here means that during task runtime estimations, file transfer estimation is taken into account.
 */
public class StorageAwareSPSS extends SPSS {
    public StorageAwareSPSS(double budget, double deadline, List<DAG> dags, double alpha, AlgorithmStatistics ensembleStatistics,
                            Environment environment, CloudSimWrapper cloudsim, VMType vmType) {
        super(budget, deadline, dags, alpha, ensembleStatistics, environment, cloudsim, vmType);
    }

    @Override
    protected CriticalPath newCriticalPath(TopologicalOrder order, HashMap<Task, Double> runtimes) {
        final Environment environment = getEnvironment();
        return new StorageAwareCriticalPath(order, runtimes, getVmType(), environment);
    }

    @Override
    protected double getPredictedTaskRuntime(Task task) {
        final Environment environment = getEnvironment();
        return environment.getComputationPredictedRuntimeForSingleTask(getVmType(), task) + environment.getTotalTransferTimeEstimation(task);
    }
}
