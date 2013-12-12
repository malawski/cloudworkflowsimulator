package cws.core.engine;

import cws.core.core.VMType;
import cws.core.dag.DAG;
import cws.core.dag.Task;
import cws.core.storage.StorageManager;
import cws.core.storage.StorageManagerStatistics;

public class Environment {
    private VMType vmType;
    private StorageManager storageManager;

    /**
     * The prediction strategy, i.e. how we will predict task's runtime.
     */
    private PredictionStrategy predictionStrategy;

    public Environment(VMType vmType, StorageManager storageManager, PredictionStrategy predictionStrategy) {
        this.vmType = vmType;
        this.storageManager = storageManager;
        this.predictionStrategy = predictionStrategy;
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
    public double getPredictedRuntime(Task task) {
        return predictionStrategy.getPredictedRuntime(task, vmType, storageManager);
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

    public PredictionStrategy getPredictionStrategy() {
        return predictionStrategy;
    }

    public double getProvisioningDelays() {
        return vmType.getProvisioningDelay().sample() + vmType.getDeprovisioningDelay().sample();
    }

    public double getOnlyDeprovisioningDelay() {
        return vmType.getProvisioningDelay().sample();
    }
}
