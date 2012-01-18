package cws.core.algorithms;

import java.util.List;

import cws.core.dag.DAG;
import cws.core.provisioner.SimpleUtilizationBasedProvisioner;
import cws.core.scheduler.WorkflowAwareEnsembleScheduler;

public class WADPDS extends DynamicAlgorithm {
    public WADPDS(double budget, double deadline, List<DAG> dags, double price, double maxScaling) {
        super(budget, deadline, dags, price, 
                new WorkflowAwareEnsembleScheduler(), 
                new SimpleUtilizationBasedProvisioner(maxScaling));
    }
}
