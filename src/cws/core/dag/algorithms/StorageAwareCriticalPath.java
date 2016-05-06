package cws.core.dag.algorithms;

import cws.core.core.VMType;
import cws.core.dag.Task;
import cws.core.storage.StorageManager;

import java.util.Map;


/**
 * Storage aware version of {@link CriticalPath}.
 * 
 * Storage awareness here means that during task runtime estimations, file transfer estimation is taken into account.
 */
public class StorageAwareCriticalPath extends CriticalPath {

    final private StorageManager storageManager;

    public StorageAwareCriticalPath(TopologicalOrder order, Map<Task, Double> runtimes,
            VMType vmType,  StorageManager storageManager) {
        super(order, runtimes, vmType);

        this.storageManager = storageManager;
    }


    @Override
    protected double getPredictedTaskRuntime(Task task, VMType vmType) {
        return vmType.getPredictedTaskRuntime(task)
                + this.storageManager.getTotalTransferTimeEstimation(task);
    }
}
