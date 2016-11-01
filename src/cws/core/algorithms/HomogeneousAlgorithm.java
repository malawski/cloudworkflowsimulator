package cws.core.algorithms;

import java.util.List;

import com.google.common.base.Preconditions;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.core.VMType;
import cws.core.dag.DAG;
import cws.core.engine.Environment;

/**
 * A base class for scheduling/planning algorithms when all VMs are
 * identical.
 */
public abstract class HomogeneousAlgorithm extends Algorithm  {

    /** Environment of simulation (VMs, storage info) */
    private final Environment environment;

    public HomogeneousAlgorithm (double budget, double deadline, List<DAG> dags,
                                 AlgorithmStatistics algorithmStatistics,
                                 Environment environment, CloudSimWrapper cloudsim, VMType representativeVmType) {
        super(budget, deadline, dags, algorithmStatistics, cloudsim, representativeVmType);
        this.environment = environment;
    }

    public final Environment getEnvironment() {
        return this.environment;
    }
}
