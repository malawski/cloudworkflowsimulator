package cws.core.algorithms;

import java.util.List;

import cws.core.cloudsim.CloudSimWrapper;
import cws.core.dag.DAG;
import cws.core.engine.Environment;

public class MaxMin extends MinMin {
    public MaxMin(double budget, double deadline, List<DAG> dags, Environment environment,
            AlgorithmStatistics ensembleStatistics, CloudSimWrapper cloudsim) {
        super(budget, deadline, dags, ensembleStatistics, environment, cloudsim);
    }

    /**
     * return the more expensive solution (MaxMin)
     */
    @Override
    Solution bestSolution(Solution a, Solution b) {
        if (b == null)
            return a;

        if (a.cost > b.cost)
            return a;

        if (a.cost == b.cost && a.slot.start < b.slot.start)
            return a;

        return b;
    }
}
