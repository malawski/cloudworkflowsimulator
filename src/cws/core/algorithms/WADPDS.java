package cws.core.algorithms;

import java.util.List;

import cws.core.Scheduler;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.core.VMType;
import cws.core.dag.DAG;
import cws.core.engine.Environment;
import cws.core.provisioner.HomogeneousProvisioner;
import cws.core.provisioner.SimpleUtilizationBasedProvisioner;
import cws.core.scheduler.ComputationOnlyRuntimePredictioner;
import cws.core.scheduler.RuntimeWorkflowAdmissioner;
import cws.core.scheduler.WorkflowAwareEnsembleScheduler;

public class WADPDS extends DynamicAlgorithm {
    public WADPDS(double budget, double deadline, List<DAG> dags, AlgorithmStatistics ensembleStatistics, Environment environment,
                  CloudSimWrapper cloudsim, Scheduler scheduler, HomogeneousProvisioner provisioner) {
        super(budget, deadline, dags, ensembleStatistics, environment, cloudsim, scheduler, provisioner);
    }
}
