package cws.core.algorithms;

import java.util.List;

import cws.core.Scheduler;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.core.VMType;
import cws.core.dag.DAG;
import cws.core.engine.Environment;
import cws.core.provisioner.HomogeneousProvisioner;
import cws.core.provisioner.SimpleUtilizationBasedProvisioner;
import cws.core.scheduler.ComputationAndTransfersRuntimePredictioner;
import cws.core.scheduler.RuntimeWorkflowAdmissioner;
import cws.core.scheduler.WorkflowAndLocalityAwareEnsembleScheduler;

/**
 * Storage and file locality aware version of WADPDS algorithm.
 * 
 * Storage awareness here means that during task runtime estimations, file transfer estimation is taken into account.
 * 
 * File locality means that the scheduler tries to minimize the number of file transfers between tasks.
 */
public class StorageAndLocalityAwareWADPDS extends DynamicAlgorithm {
    public StorageAndLocalityAwareWADPDS(double budget, double deadline, List<DAG> dags, AlgorithmStatistics ensembleStatistics,
                                         Environment environment, CloudSimWrapper cloudsim, VMType vmType,
                                         Scheduler scheduler, HomogeneousProvisioner provisioner) {
        super(budget, deadline, dags, ensembleStatistics, environment, cloudsim, vmType, scheduler, provisioner);
    }
}
