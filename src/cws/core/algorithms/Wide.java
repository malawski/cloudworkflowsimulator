package cws.core.algorithms;

import java.util.List;

import cws.core.dag.DAG;

/**
 * @author Gideon Juve <juve@usc.edu>
 */
public class Wide extends Backtrack {

    public Wide(double budget, double deadline, List<DAG> dags) {
        super(budget, deadline, dags);
    }

    @Override
    public void plan() {
        // Estimate number of nodes we can use
        double hours = getDeadline() / (60 * 60);
        double nodeHours = getBudget() / VMType.SMALL.price;
        int N = (int) Math.floor(nodeHours / hours);

        // Add them to the initial plan
        Plan plan = getPlan();
        for (int i = 0; i < N; i++) {
            plan.resources.add(new Resource(VMType.SMALL));
        }

        super.plan();
    }
}
