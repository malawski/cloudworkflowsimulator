package cws.core.algorithms;

import java.util.List;

import cws.core.cloudsim.CloudSimWrapper;
import cws.core.dag.DAG;
import cws.core.engine.Environment;

/**
 * @author Gideon Juve <juve@usc.edu>
 */
public class Wide extends Backtrack {

    public static final int SECONDS_IN_HOUR = 60 * 60;

    public Wide(double budget, double deadline, List<DAG> dags, Environment environment,
            AlgorithmStatistics ensembleStatistics, CloudSimWrapper cloudsim) {
        super(budget, deadline, dags, environment, ensembleStatistics, cloudsim);
    }

    @Override
    public void plan() {
        // Estimate number of nodes we can use
        double hours = getDeadline() / SECONDS_IN_HOUR;
        double nodeHours = getBudget() / environment.getSingleVMPrice();
        int N = (int) Math.floor(nodeHours / hours);

        // Add them to the initial plan
        Plan plan = getPlan();
        for (int i = 0; i < N; i++) {
            plan.resources.add(new Resource(environment));
        }

        super.plan();
    }
}
