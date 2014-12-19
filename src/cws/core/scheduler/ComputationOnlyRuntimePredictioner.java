package cws.core.scheduler;

import cws.core.VM;
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
    public double getPredictedRuntime(Task task, VM vm) {
        return environment.getComputationPredictedRuntime(task);
    }

    @Override
    public double getPredictedRuntime(DAG dag) {
        return environment.getComputationPredictedRuntime(dag);
    }
}
