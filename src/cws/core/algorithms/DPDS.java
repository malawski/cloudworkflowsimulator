package cws.core.algorithms;

import java.util.List;

import cws.core.cloudsim.CloudSimWrapper;
import cws.core.dag.DAG;
import cws.core.engine.Environment;
import cws.core.provisioner.SimpleUtilizationBasedProvisioner;
import cws.core.scheduler.EnsembleDynamicScheduler;

public class DPDS extends DynamicAlgorithm {
    public DPDS(double budget, double deadline, List<DAG> dags, double maxScaling, Environment environment,
            AlgorithmStatistics ensembleStatistics, CloudSimWrapper cloudsim) {
        super(budget, deadline, dags, new EnsembleDynamicScheduler(cloudsim, environment),
                new SimpleUtilizationBasedProvisioner(maxScaling, cloudsim, environment), environment,
                ensembleStatistics, cloudsim);
    }
}
