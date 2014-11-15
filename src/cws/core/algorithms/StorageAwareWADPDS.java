package cws.core.algorithms;

import java.util.List;

import cws.core.cloudsim.CloudSimWrapper;
import cws.core.dag.DAG;
import cws.core.engine.Environment;
import cws.core.provisioner.SimpleUtilizationBasedProvisioner;
import cws.core.scheduler.WorkflowAndStorageAwareEnsembleScheduler;

public class StorageAwareWADPDS extends DynamicAlgorithm {
    public StorageAwareWADPDS(double budget, double deadline, List<DAG> dags, double maxScaling,
            AlgorithmStatistics ensembleStatistics, Environment environment, CloudSimWrapper cloudsim) {
        super(budget, deadline, dags, new WorkflowAndStorageAwareEnsembleScheduler(cloudsim, environment),
                new SimpleUtilizationBasedProvisioner(maxScaling, cloudsim), ensembleStatistics, environment, cloudsim);
    }
}
