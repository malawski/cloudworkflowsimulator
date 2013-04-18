package cws.core.algorithms;

import java.util.List;

import cws.core.dag.DAG;
import cws.core.emulator.CloudEmulator;
import cws.core.provisioner.SimpleUtilizationBasedProvisioner;
import cws.core.scheduler.EnsembleDynamicScheduler;

public class DPDS extends DynamicAlgorithm {
    public DPDS(double budget, double deadline, List<DAG> dags, double price, double maxScaling) {
        super(budget, deadline, dags, price, new EnsembleDynamicScheduler(new CloudEmulator()), new SimpleUtilizationBasedProvisioner(
                maxScaling));
    }
}
