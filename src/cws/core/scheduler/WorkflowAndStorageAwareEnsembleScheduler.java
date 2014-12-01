package cws.core.scheduler;

import cws.core.cloudsim.CloudSimWrapper;
import cws.core.dag.DAG;
import cws.core.dag.Task;
import cws.core.engine.Environment;

/**
 * {@link WorkflowAwareEnsembleScheduler} implementation that is also aware of the underlying storage.
 */
public class WorkflowAndStorageAwareEnsembleScheduler extends WorkflowAwareEnsembleScheduler {
    public WorkflowAndStorageAwareEnsembleScheduler(CloudSimWrapper cloudsim, Environment environment) {
        super(cloudsim, environment);
    }

    protected double getPredictedRuntime(Task task) {
        return environment.getComputationPredictedRuntime(task)
                + environment.getStorageManager().getTransferTimeEstimation(task);
    }

    protected double getPredictedRuntime(DAG dag) {
        return environment.getComputationPredictedRuntime(dag)
                + environment.getStorageManager().getTransferTimeEstimation(dag);
    }
}
