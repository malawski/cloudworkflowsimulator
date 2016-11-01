package cws.core.algorithms;

import java.util.List;

import cws.core.Provisioner;
import cws.core.Scheduler;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.core.VMType;
import cws.core.dag.DAG;
import cws.core.engine.Environment;
import cws.core.provisioner.HomogeneousProvisioner;
import cws.core.provisioner.SimpleUtilizationBasedProvisioner;
import cws.core.scheduler.EnsembleDynamicScheduler;

public class DPDS extends DynamicAlgorithm {
    public DPDS(double budget, double deadline, List<DAG> dags, AlgorithmStatistics ensembleStatistics, Environment environment,
                CloudSimWrapper cloudsim, VMType vmType, Scheduler scheduler, HomogeneousProvisioner provisioner) {
        super(budget, deadline, dags, ensembleStatistics, environment, cloudsim, vmType, scheduler, provisioner);
    }
}
