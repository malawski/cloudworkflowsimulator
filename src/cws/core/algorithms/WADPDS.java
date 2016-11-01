package cws.core.algorithms;

import java.util.List;

import cws.core.cloudsim.CloudSimWrapper;
import cws.core.core.VMType;
import cws.core.dag.DAG;
import cws.core.engine.Environment;
import cws.core.provisioner.SimpleUtilizationBasedProvisioner;
import cws.core.scheduler.ComputationOnlyRuntimePredictioner;
import cws.core.scheduler.RuntimeWorkflowAdmissioner;
import cws.core.scheduler.WorkflowAwareEnsembleScheduler;

public class WADPDS extends DynamicAlgorithm {
    public WADPDS(double budget, double deadline, List<DAG> dags, double maxScaling,
                  AlgorithmStatistics ensembleStatistics, Environment environment, CloudSimWrapper cloudsim, VMType representativeVmType) {
        super(budget, deadline, dags, new WorkflowAwareEnsembleScheduler(cloudsim, environment,
                        new RuntimeWorkflowAdmissioner(cloudsim, new ComputationOnlyRuntimePredictioner(environment),
                                environment, representativeVmType), representativeVmType), new SimpleUtilizationBasedProvisioner(maxScaling, cloudsim, representativeVmType), ensembleStatistics,
                environment, cloudsim, representativeVmType);
    }
}
