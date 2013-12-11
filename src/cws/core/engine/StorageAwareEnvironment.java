package cws.core.engine;

import cws.core.core.VMType;
import cws.core.dag.Task;
import cws.core.storage.StorageManager;

/**
 * Storage aware environment. During runtime predictions it takes transfers into account.
 */
public class StorageAwareEnvironment extends Environment {
    public StorageAwareEnvironment(VMType vmType, StorageManager storageManager) {
        super(vmType, storageManager);
    }

    /**
     * Storage aware version of runtime prediction.
     * 
     * @see cws.core.engine.Environment#getPredictedRuntime(cws.core.dag.Task)
     */
    @Override
    public double getPredictedRuntime(Task task) {
        return getComputationTaskEstimation(task) + storageManager.getTransferTimeEstimation(task);
    }
}
