package cws.core.engine;

import java.util.concurrent.TimeUnit;

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

    // FIXME(mequrel): temporary encapsulation breakage for static algorithm, dynamic algorithm and provisioners
    public cws.core.core.VMType getVMType() {
        return vmType;
    }

    /**
     * Returns task's predicted runtime. It is based on vmType and storage manager. <br>
     * Note that the estimation is trivial and may not be accurate during congestion and it doesn't include runtime
     * variance.
     * 
     * @return task's predicted runtime as a double
     */

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

    public double getVMCostFor(double runtimeInSeconds) {
        double hours = runtimeInSeconds / TimeUnit.HOURS.toSeconds(1);
        int fullHours = (int) Math.ceil(hours);
        return Math.max(1, fullHours) * vmType.getPrice();
    }

    public double getSingleVMPrice() {
        return vmType.getPrice();
    }
}
