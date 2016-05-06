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
        return environment.getComputationPredictedRuntime(task)
                + environment.getStorageManager().getTotalTransferTimeEstimation(task, vm);
    }

    @Override
    public double getPredictedRuntime(DAG dag) {
        return environment.getComputationPredictedRuntime(dag)
                + environment.getStorageManager().getTotalTransferTimeEstimation(dag);
    }
}
