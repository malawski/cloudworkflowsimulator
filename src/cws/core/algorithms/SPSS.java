package cws.core.algorithms;

import java.util.*;

import cws.core.cloudsim.CloudSimWrapper;
import cws.core.dag.DAG;
import cws.core.dag.Task;
import cws.core.dag.algorithms.TopologicalOrder;
import cws.core.engine.Environment;

/**
 * @author Gideon Juve <juve@usc.edu>
 */
public class SPSS extends StaticAlgorithm {

    /** Tuning parameter for deadline distribution (low alpha = runtime, high alpha = tasks) */
    private double alpha;

    public SPSS(double budget, double deadline, List<DAG> dags, double alpha, Environment environment,
            AlgorithmStatistics ensembleStatistics, CloudSimWrapper cloudsim) {
        super(budget, deadline, dags, ensembleStatistics, environment, cloudsim);
        this.alpha = alpha;
    }

    /**
     * Develop a plan for a single DAG
     */
    @Override
    Plan planDAG(DAG dag, Plan currentPlan) throws NoFeasiblePlan {
        HashMap<Task, VMType> vmTypes = new HashMap<Task, VMType>();
        HashMap<Task, Double> runtimes = new HashMap<Task, Double>();
        TopologicalOrder order = computeTopologicalOrder(dag, vmTypes, runtimes);

        /**
         * FIXME Later we will determine the best VM type for each task
         * 
         * <pre>
         * assignEachTaskToCheapestResource()
         * criticalPath = computeCriticalPath()
         * while (criticalPath > deadline) {
         * upgradeTaskWithBestBangForBuck()
         * criticalPath = computeCriticalPath()
         * }
         * </pre>
         */

        // Get deadlines for each task (deadline distribution)
        final HashMap<Task, Double> deadlines = getDeadlineDistribution(order, runtimes, this.alpha);

        // Sort tasks by deadline
        LinkedList<Task> sortedTasks = new LinkedList<Task>();
        for (Task t : order) {
            sortedTasks.add(t);
        }
        Comparator<Task> deadlineComparator = new Comparator<Task>() {
            @Override
            public int compare(Task t1, Task t2) {
                double d1 = deadlines.get(t1);
                double d2 = deadlines.get(t2);
                if (d1 < d2) {
                    return -1;
                } else if (d1 > d2) {
                    return 1;
                } else {
                    return 0;
                }
            }
        };
        Collections.sort(sortedTasks, deadlineComparator);

        // Create a new plan
        Plan plan = new Plan(currentPlan);

        // Actual finish times of tasks
        HashMap<Task, Double> finishTimes = new HashMap<Task, Double>();

        // Assign resources to each task
        for (Task task : sortedTasks) {
            double runtime = runtimes.get(task);
            double deadline = deadlines.get(task);
            VMType vmtype = vmTypes.get(task);

            // Compute earliest start time of task
            double earliestStart = 0.0;
            for (Task parent : task.getParents()) {
                earliestStart = Math.max(earliestStart, finishTimes.get(parent));
            }

            Solution newResource;

            // Best scheduling solution for task
            Solution best;
            {
                // Default is to allocate a new resource
                Resource r = new Resource(vmtype);
                double cost = r.getCostWith(earliestStart, earliestStart + runtime);
                Slot sl = new Slot(task, earliestStart, runtime);
                best = newResource = new Solution(r, sl, cost, true);
            }

            // Check each resource for a better (cheaper, earlier) solution
            for (Resource r : plan.resources) {

                // The resource must match the vm type of the task
                if (vmtype != r.vmtype) {
                    continue;
                }

                // Try placing task at the beginning of resource schedule
                if (earliestStart + runtime < r.getStart()) {

                    // Option 1: Leave no gap
                    nogap: {
                        double ast = r.getStart() - runtime;
                        if (ast < earliestStart) {
                            break nogap;
                        }

                        double aft = ast + runtime;
                        if (aft > deadline || aft > r.getStart()) {
                            break nogap;
                        }

                        double cost = r.getCostWith(ast, r.getEnd()) - r.getCost();
                        Slot slot = new Slot(task, ast, runtime);
                        Solution soln = new Solution(r, slot, cost, false);
                        if (soln.betterThan(best)) {
                            best = soln;
                        }
                    }

                    // Option 2: Leave a big gap
                    biggap: {
                        int runtimeHours = (int) Math.ceil(runtime / (60 * 60));

                        double ast = r.getStart() - (runtimeHours * 60 * 60);
                        if (ast < earliestStart) {
                            ast = earliestStart;
                        }

                        double aft = ast + runtime;
                        if (aft > deadline || aft > r.getStart()) {
                            break biggap;
                        }

                        double cost = r.getCostWith(ast, r.getEnd()) - r.getCost();
                        Slot sl = new Slot(task, ast, runtime);
                        Solution soln = new Solution(r, sl, cost, false);
                        if (soln.betterThan(best)) {
                            best = soln;
                        }
                    }

                    // Option 3: Use some slack time (medium gap)
                    slack: {
                        double slack = (r.getHours() * 60 * 60) - (r.getEnd() - r.getStart());

                        double ast = r.getStart() - slack;
                        if (ast < earliestStart) {
                            ast = earliestStart;
                        }

                        double aft = ast + runtime;
                        if (aft > deadline || aft > r.getStart()) {
                            break slack;
                        }

                        // This solution should be free because we add no hours
                        double cost = r.getCostWith(ast, r.getEnd()) - r.getCost();
                        if (cost > 1e-6) {
                            throw new RuntimeException("Solution should be free");
                        }
                        Slot sl = new Slot(task, ast, runtime);
                        Solution soln = new Solution(r, sl, cost, false);
                        if (soln.betterThan(best)) {
                            best = soln;
                        }
                    }
                }

                // Try placing it in a gap in the schedule
                double lastEnd = -1;
                gap: for (double start : r.getStartTimes()) {
                    double begin = lastEnd;
                    double end = start;

                    lastEnd = start + r.schedule.get(start).duration;

                    // This just skips the first occupied slot
                    if (begin < 0) {
                        continue gap;
                    }

                    if (begin == end) {
                        continue gap;
                    }

                    // Sanity check
                    if (begin > end && begin - end > 1e-9) {
                        throw new RuntimeException("Negative sized empty slot");
                    }

                    double ast;
                    if (begin >= earliestStart) {
                        ast = begin;
                    } else {
                        ast = earliestStart;
                    }

                    double aft = ast + runtime;
                    if (aft <= end && aft <= deadline) {
                        double cost = 0.0; // free as in beer
                        Slot sl = new Slot(task, ast, runtime);
                        Solution soln = new Solution(r, sl, cost, false);
                        if (soln.betterThan(best)) {
                            best = soln;
                        }

                        // We won't find a better solution by looking at later
                        // gaps, so we can stop looking here
                        break gap;
                    }
                }

                // Try to placing it at the end of the schedule
                atend: if (r.getEnd() + runtime < deadline) {

                    // Actual start time
                    double ast;
                    if (earliestStart < r.getEnd()) {
                        ast = r.getEnd();
                    } else {
                        ast = earliestStart;
                    }

                    // Actual finish time
                    double aft = ast + runtime;
                    if (aft > deadline) {
                        break atend;
                    }

                    double cost = r.getCostWith(r.getStart(), aft) - r.getCost();
                    Slot sl = new Slot(task, ast, runtime);
                    Solution soln = new Solution(r, sl, cost, false);
                    if (soln.betterThan(best)) {
                        best = soln;
                    }
                }
            }

            if (newResource.cost < best.cost) {
                getCloudsim().log(
                        String.format("%s best: %f %s\n", task.getId(), best.cost, newResource.betterThan(best)));
            }

            // Schedule task on resource of best solution
            best.addToPlan(plan);

            // Save actual finish time of task
            finishTimes.put(task, best.slot.start + runtime);
        }

        return plan;
    }
}
