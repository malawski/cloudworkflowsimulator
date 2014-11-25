package cws.core.engine;

import cws.core.core.VMType;
import cws.core.dag.DAG;
import cws.core.dag.Task;
import cws.core.storage.StorageManager;
import cws.core.storage.StorageManagerStatistics;

public class Environment {
    private final VMType vmType;
    private final StorageManager storageManager;

    public Environment(VMType vmType, StorageManager storageManager) {
        this.vmType = vmType;
        this.storageManager = storageManager;
    }

    // FIXME(mequrel): temporary encapsulation breakage for static algorithm, dynamic algorithm and provisioners
    public VMType getVMType() {
        return vmType;
    }

    /**
     * Returns task's predicted runtime. It is based on vmType and storage manager. <br>
     * Note that the estimation is trivial and may not be accurate during congestion and it doesn't include runtime
     * variance.
     *
     * @return task's predicted runtime as a double
     */
    public double getComputationPredictedRuntime(Task task) {
        return vmType.getPredictedTaskRuntime(task);
    }

    public double getComputationPredictedRuntime(DAG dag) {
        double sum = 0.0;
        for (String taskName : dag.getTasks()) {
            sum += getComputationPredictedRuntime(dag.getTaskById(taskName));
        }
        return sum;
    }
    
    public double getTransfersPredictedRuntime(Task task) {
        return storageManager.getTransferTimeEstimation(task);
    }
   
    public double getTransfersPredictedRuntime(DAG dag) {
        double sum = 0.0;
        for (String taskName : dag.getTasks()) {
            sum += getTransfersPredictedRuntime(dag.getTaskById(taskName));
        }
        return sum;
    }
    
    public StorageManagerStatistics getStorageManagerStatistics() {
        return storageManager.getStorageManagerStatistics();
    }

    public double getVMCostFor(double runtimeInSeconds) {
        double billingUnits = runtimeInSeconds / getVMType().getBillingTimeInSeconds();
        int fullBillingUnits = (int) Math.ceil(billingUnits);
        return Math.max(1, fullBillingUnits) * vmType.getPriceForBillingUnit();
    }

    public double getSingleVMPrice() {
        return vmType.getPriceForBillingUnit();
    }

    public double getBillingTimeInSeconds() {
        return vmType.getBillingTimeInSeconds();
    }

    public double getVMProvisioningOverallDelayEstimation() {
        return vmType.getProvisioningDelay().sample() + vmType.getDeprovisioningDelay().sample();
    }

    public double getDeprovisioningDelayEstimation() {
        return vmType.getDeprovisioningDelay().sample();
    }
}
