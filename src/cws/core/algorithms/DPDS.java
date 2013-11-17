package cws.core.algorithms;

import java.util.List;

import cws.core.AlgorithmStatistics;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.dag.DAG;
import cws.core.provisioner.SimpleUtilizationBasedProvisioner;
import cws.core.scheduler.EnsembleDynamicScheduler;
import cws.core.storage.StorageManager;

public class DPDS extends DynamicAlgorithm {
    public DPDS(double budget, double deadline, List<DAG> dags, double price, double maxScaling,
            StorageManager storageManager, AlgorithmStatistics ensembleStatistics, CloudSimWrapper cloudsim) {
        super(budget, deadline, dags, price, new EnsembleDynamicScheduler(cloudsim),
                new SimpleUtilizationBasedProvisioner(maxScaling, cloudsim), storageManager, ensembleStatistics,
                cloudsim);
    }
}
