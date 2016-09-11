package cws.core.dag.algorithms;

import java.util.Map;

import cws.core.core.VMType;
import cws.core.dag.Task;
import cws.core.engine.Environment;

/**
 * Storage aware version of {@link CriticalPath}.
 * 
 * Storage awareness here means that during task runtime estimations, file transfer estimation is taken into account.
 */
public class StorageAwareCriticalPath extends CriticalPath {

    private final Environment environment;

    public StorageAwareCriticalPath(TopologicalOrder order, Map<Task, Double> runtimes, VMType vmType,
            Environment environment) {
        super(order, runtimes, vmType);

        this.environment = environment;
    }

    @Override
    protected double getPredictedTaskRuntime(Task task, VMType vmType) {
        return vmType.getPredictedTaskRuntime(task) + this.environment.getTotalTransferTimeEstimation(task);
    }
}
