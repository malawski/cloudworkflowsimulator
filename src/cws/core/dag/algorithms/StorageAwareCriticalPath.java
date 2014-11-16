package cws.core.dag.algorithms;

import java.util.Map;

import cws.core.dag.Task;
import cws.core.engine.Environment;

/**
 * Storage aware version of {@link CriticalPath}.
 * 
 * Storage awareness here means that during task runtime estimations, file transfer estimation is taken into account.
 */
public class StorageAwareCriticalPath extends CriticalPath {
    public StorageAwareCriticalPath(TopologicalOrder order, Map<Task, Double> runtimes, Environment environment) {
        super(order, runtimes, environment);
    }

    @Override
    protected double getPredictedTaskRuntime(Environment environment, Task task) {
        return environment.getComputationPredictedRuntime(task) + environment.getTransfersPredictedRuntime(task);
    }
}
