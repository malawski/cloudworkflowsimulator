package cws.core.engine;

import cws.core.core.VMType;

import cws.core.dag.Task;
import cws.core.storage.StorageManager;

/**
 * Storage aware prediction strategy. It takes file transfers into account.
 */
public class StorageAwarePredictionStrategy implements PredictionStrategy {

    /**
     * Storage aware version of runtime prediction. It takes file transfers into account.
     * 
     * @see ({@link #getPredictedRuntime(Task, VMType, StorageManager)}
     */
    @Override
    public double getPredictedRuntime(Task task, VMType vmType, StorageManager storageManager) {
        return task.getSize() / vmType.getMips() + storageManager.getTransferTimeEstimation(task);
    }
}
