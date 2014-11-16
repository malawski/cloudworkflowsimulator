package cws.core.algorithms;

import java.util.List;

import cws.core.cloudsim.CloudSimWrapper;
import cws.core.dag.DAG;
import cws.core.engine.Environment;
import cws.core.provisioner.SimpleUtilizationBasedProvisioner;
import cws.core.scheduler.WorkflowAndStorageAwareEnsembleScheduler;

/**
 * Storage aware version of WADPDS algorithm.
 * 
 * Storage awareness here means that during task runtime estimations, file transfer estimation is taken into account.
 */
public class StorageAwareWADPDS extends DynamicAlgorithm {
    public StorageAwareWADPDS(double budget, double deadline, List<DAG> dags, double maxScaling,
            AlgorithmStatistics ensembleStatistics, Environment environment, CloudSimWrapper cloudsim) {
        super(budget, deadline, dags, new WorkflowAndStorageAwareEnsembleScheduler(cloudsim, environment),
                new SimpleUtilizationBasedProvisioner(maxScaling, cloudsim), ensembleStatistics, environment, cloudsim);
    }
}
