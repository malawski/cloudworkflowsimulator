package cws.core.engine;

import cws.core.core.VMType;
import cws.core.dag.Task;
import cws.core.storage.StorageManager;

/**
 * Storage unaware prediction strategy. It ignores file transfers.
 */
public class StorageUnawarePredictionStrategy implements PredictionStrategy {
    /**
     * Storage unaware version of runtime prediction. It ignores file transfers.
     * 
     * @see ({@link #getPredictedRuntime(Task, VMType, StorageManager)}
     */
    @Override
    public double getPredictedRuntime(Task task, VMType vmType, StorageManager storageManager) {
        return task.getSize() / vmType.getMips();
    }
}
