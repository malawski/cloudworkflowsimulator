package cws.core.engine;

import cws.core.algorithms.VMType;
import cws.core.dag.DAG;
import cws.core.dag.Task;
import cws.core.storage.StorageManager;
import cws.core.storage.StorageManagerStatistics;

public class Environment {

    private cws.core.core.VMType vmType;
    private StorageManager storageManager;

    public Environment(cws.core.core.VMType vmType, StorageManager storageManager) {
        this.vmType = vmType;
        this.storageManager = storageManager;
    }

    /**
     * Returns task's predicted runtime. It is based on task's vmType and provided storage manager. <br>
     * Note that the estimation is trivial and may not be accurate during congestion and it doesn't include runtime
     * variance.
     * 
     * @param storageManager manager used to estimate transfers
     * @return task's predicted runtime as a double
     */
    public double getPredictedRuntime(Task task, StorageManager storageManager, VMType vmType) {
        return task.getSize() / vmType.getMips() + storageManager.getTransferTimeEstimation(task);
    }

    public double getPredictedRuntime(Task task, StorageManager storageManager, cws.core.core.VMType vmType) {
        return task.getSize() / vmType.getMips() + storageManager.getTransferTimeEstimation(task);
    }

    public double getPredictedRuntime(Task task) {
        return getComputationTaskEstimation(task) + storageManager.getTransferTimeEstimation(task);
    }

    // TODO(mequrel): Maybe should be moved to VMType?
    private double getComputationTaskEstimation(Task task) {
        return task.getSize() / vmType.getMips();
    }

    public double getPredictedRuntime(DAG dag) {
        double sum = 0.0;
        for (String taskName : dag.getTasks()) {
            sum += getPredictedRuntime(dag.getTaskById(taskName));
        }
        return sum;
    }

    public StorageManagerStatistics getStorageManagerStatistics() {
        return storageManager.getStorageManagerStatistics();
    }
}
