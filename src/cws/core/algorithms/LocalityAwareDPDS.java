package cws.core.algorithms;

import cws.core.Scheduler;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.dag.DAG;
import cws.core.engine.Environment;
import cws.core.provisioner.HomogeneousProvisioner;

import java.util.List;

/**
 * DPDS version that is aware of file locality.
 */
public class LocalityAwareDPDS extends DynamicAlgorithm {
    public LocalityAwareDPDS(double budget, double deadline, List<DAG> dags, AlgorithmStatistics ensembleStatistics,
                             Environment environment, CloudSimWrapper cloudsim, Scheduler scheduler,
                             HomogeneousProvisioner provisioner) {
        super(budget, deadline, dags, ensembleStatistics, environment, cloudsim, scheduler, provisioner);
    }
}
