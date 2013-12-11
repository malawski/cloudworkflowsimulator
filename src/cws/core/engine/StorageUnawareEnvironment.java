package cws.core.engine;

import cws.core.core.VMType;
import cws.core.dag.Task;
import cws.core.storage.StorageManager;

/**
 * Storage unaware implementation of {@link Environment}. It does not take transfers into account when estimates
 * task's runtime.
 */
public class StorageUnawareEnvironment extends Environment {
    public StorageUnawareEnvironment(VMType vmType, StorageManager storageManager) {
        super(vmType, storageManager);
    }

    /**
     * Predicts the runtime of a task, ignoring storage transfers.
     * 
     * @see cws.core.engine.Environment#getPredictedRuntime(cws.core.dag.Task)
     */
    @Override
    public double getPredictedRuntime(Task task) {
        return getComputationTaskEstimation(task);
    }
}
