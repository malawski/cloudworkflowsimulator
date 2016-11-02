package cws.core.scheduler;

import cws.core.VM;
import cws.core.core.VMType;
import cws.core.dag.DAG;
import cws.core.dag.Task;
import cws.core.engine.Environment;

/**
 * {@link RuntimePredictioner} that only takes computation into account.
 */
public final class ComputationOnlyRuntimePredictioner implements RuntimePredictioner {
    private final Environment environment;

    public ComputationOnlyRuntimePredictioner(Environment environment) {
        this.environment = environment;
    }

    @Override
    public double getPredictedRuntime(Task task, VM vm, VMType vmType) {
        return environment.getComputationPredictedRuntimeForSingleTask(vmType, task);
    }

    @Override
    public double getPredictedRuntime(DAG dag, VM vm) {
        return environment.getComputationPredictedRuntimeForDAG(vm.getVmType(), dag);
    }
}
