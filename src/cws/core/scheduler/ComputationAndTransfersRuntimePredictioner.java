package cws.core.scheduler;

import cws.core.VM;
import cws.core.dag.DAG;
import cws.core.dag.Task;
import cws.core.engine.Environment;

/**
 * {@link RuntimePredictioner} that takes both computation and file transfers into account.
 */
public final class ComputationAndTransfersRuntimePredictioner implements RuntimePredictioner {
    private final Environment environment;

    public ComputationAndTransfersRuntimePredictioner(Environment environment) {
        this.environment = environment;
    }

    @Override
    public double getPredictedRuntime(Task task, VM vm) {
        return environment.getComputationPredictedRuntimeForSingleTask(vm.getVmType(), task) + environment.getTotalTransferTimeEstimation(task, vm);
    }

    @Override
    public double getPredictedRuntime(DAG dag, VM vm) {
        return environment.getComputationPredictedRuntimeForDAG(vm.getVmType(), dag) + environment.getTotalTransferTimeEstimation(dag);
    }
}
