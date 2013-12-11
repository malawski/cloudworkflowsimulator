package cws.core.engine;

import cws.core.core.VMType;
import cws.core.dag.Task;
import cws.core.storage.StorageManager;

/**
 * The task's runtime prediction strategy. Used in {@link Environment}.
 */
public interface PredictionStrategy {

    /**
     * Predicts task's runtime.
     * 
     * @param task The task to predict runtime for.
     * @param vmType The vmType this task runs on.
     * @param storageManager The underlying storageManager.
     * @return Task's predicted runtime.
     */
    double getPredictedRuntime(Task task, VMType vmType, StorageManager storageManager);
}
