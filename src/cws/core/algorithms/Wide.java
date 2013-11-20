package cws.core.algorithms;

import java.util.List;

import cws.core.cloudsim.CloudSimWrapper;
import cws.core.dag.DAG;

/**
 * @author Gideon Juve <juve@usc.edu>
 */
public class Wide extends Backtrack {
    public Wide(double budget, double deadline, List<DAG> dags, AlgorithmStatistics ensembleStatistics,
            CloudSimWrapper cloudsim) {
        super(budget, deadline, dags, ensembleStatistics, cloudsim);
    }

    @Override
    public void plan() {
        // Estimate number of nodes we can use
        double units = getDeadline() / environment.getBillingTimeInSeconds();
        double nodeUnits = getBudget() / environment.getSingleVMPrice();
        int N = (int) Math.floor(nodeUnits / units);

        // Add them to the initial plan
        Plan plan = getPlan();
        for (int i = 0; i < N; i++) {
            plan.resources.add(new Resource(environment));
        }

        super.plan();
    }
}
