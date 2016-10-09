package cws.core.engine;

import com.google.common.base.Preconditions;
import cws.core.VM;
import cws.core.VmTypeSelectionStrategy;
import cws.core.core.VMType;
import cws.core.dag.DAG;
import cws.core.dag.Task;
import cws.core.storage.StorageManager;
import cws.core.storage.StorageManagerStatistics;

import java.util.HashSet;
import java.util.Set;

/**
 * Class which represents the cloud's environment. Consists of supported VMTypes (currently only one)
 * and StorageManager which handles file transfers within the cloud.
 */
public class Environment {
    private final Set<VMType> vmTypes;
    private final StorageManager storageManager;
    private final VmTypeSelectionStrategy vmTypeSelectionStrategy;

    public Environment(Set<VMType> vmTypes, StorageManager storageManager, VmTypeSelectionStrategy vmTypeSelectionStrategy) {
        Preconditions.checkArgument(!vmTypes.isEmpty(), "Expected vmTypes set not to be empty.");
        this.vmTypes = vmTypes;
        this.storageManager = storageManager;
        this.vmTypeSelectionStrategy = vmTypeSelectionStrategy;
    }

    /**
     * Returns VMTypes supported by this cloud (currently only one type)
     */
    public Set<VMType> getVmTypes() {
        return new HashSet<VMType>(this.vmTypes);
    }

    /**
     * Returns selected VMType based on environments strategy
     */
    public VMType getRepresentativeVMType() {
        return vmTypeSelectionStrategy.selectVmType(this.vmTypes);
    }

    /**
     * Returns task's predicted computation runtime. It is based on vmType.<br>
     * Note that the estimation is trivial, may not be accurate and it doesn't include runtime variance.
     * It's assumed that task is non-divisible into many cores and it executes on one core only.
     *
     * @return task's predicted runtime as a double
     */
    public double getComputationPredictedRuntimeForSingleTask(VMType vmType, Task task) {
        return vmType.getPredictedTaskRuntime(task);
    }

    /**
     * Returns dag's predicted computation runtime. It is based on vmType.<br>
     * Note that the estimation is trivial, may not be accurate and it doesn't include runtime variance.
     * It's assumed that task is non-divisible into many cores and it executes on one core only.
     * It doesn't include critical path or any such estimations.
     *
     * @return dag's predicted runtime as a double
     */
    public double getComputationPredictedRuntimeForDAG(VMType vmType, DAG dag) {
        double sum = 0.0;
        for (String taskName : dag.getTasks()) {
            sum += getComputationPredictedRuntimeForSingleTask(vmType, dag.getTaskById(taskName));
        }
        return sum;
    }

    public StorageManagerStatistics getStorageManagerStatistics() {
        return storageManager.getStorageManagerStatistics();
    }

    /**
     * Calculates cost or running a VM for given number of seconds
     *
     * @return cost as double
     */
    public double getVMCostFor(VMType vmType, double runtimeInSeconds) {
        return vmType.getVMCostFor(runtimeInSeconds);
    }

    /**
     * To be removed when heterogeneous cloud is introduced
     */
    public double getVMTypePrice(VMType vmType) {
        return vmType.getPriceForBillingUnit();
    }

    /**
     * To be removed when new pricing models are introduced
     */
    public double getBillingTimeInSeconds(VMType vmType) {
        return vmType.getBillingTimeInSeconds();
    }

    /**
     * Returns estimated provisioning delay of a VM.
     */
    public double getVMProvisioningOverallDelayEstimation(VMType vmType) {
        return vmType.getProvisioningOverallDelayEstimation();
    }

    /**
     * Returns estimated deprovisioning delay of a VM.
     */
    public double getDeprovisioningDelayEstimation(VMType vmType) {
        return vmType.getDeprovisioningDelayEstimation();
    }

    /**
     * Calculates time needed to transfer both input and output files of a given task.
     * Transfer time estimation depends on cloud's {@link StorageManager}
     *
     * @return time as double
     */
    public double getTotalTransferTimeEstimation(Task task) {
        return this.storageManager.getTotalTransferTimeEstimation(task);
    }

    /**
     * Calculates time needed to transfer both input and output files of a given task to and from given VM.
     * Transfer time estimation depends on cloud's {@link StorageManager} and files currently stored in the VM.
     *
     * @return time as double
     */
    public double getTotalTransferTimeEstimation(final Task task, final VM vm) {
        return this.storageManager.getTotalTransferTimeEstimation(task, vm);
    }

    /**
     * Calculates time needed to transfer input files of a given task to given VM.
     * Transfer time estimation depends on cloud's {@link StorageManager} and files currently stored in the VM.
     *
     * @return time as double
     */
    public double getInputTransferTimeEstimation(Task task, VM vm) {
        return this.storageManager.getInputTransferTimeEstimation(task, vm);
    }

    /**
     * Calculates time needed to transfer output files of a given task from given VM.
     * Transfer time estimation depends on cloud's {@link StorageManager}.
     *
     * @return time as double
     */
    public double getOutputTransferTimeEstimation(Task task, VM vm) {
        return this.storageManager.getOutputTransferTimeEstimation(task, vm);
    }

    /**
     * Calculates time needed to transfer both input and output files of all tasks of a given DAG
     * Transfer time estimation depends on cloud's {@link StorageManager}
     *
     * @return time as double
     */
    public double getTotalTransferTimeEstimation(DAG dag) {
        return this.storageManager.getTotalTransferTimeEstimation(dag);
    }

    public boolean isHomogeneous() {
        return this.vmTypes.size() == 1;
    }
}
