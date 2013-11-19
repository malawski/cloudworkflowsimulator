package cws.core.algorithms;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;

import cws.core.cloudsim.CloudSimWrapper;
import cws.core.dag.DAG;
import cws.core.dag.Task;
import cws.core.dag.algorithms.TopologicalOrder;
import cws.core.storage.StorageManager;

/**
 * @author Gideon Juve <juve@usc.edu>
 */
public class MinMin extends StaticAlgorithm {

    public MinMin(double budget, double deadline, List<DAG> dags, AlgorithmStatistics ensembleStatistics,
            StorageManager storageManager, CloudSimWrapper cloudsim) {
        super(budget, deadline, dags, ensembleStatistics, storageManager, cloudsim);
    }

    /**
     * Develop a plan for a single DAG
     */
    @Override
    Plan planDAG(DAG dag, Plan currentPlan) throws NoFeasiblePlan {
        HashMap<Task, VMType> vmTypes = new HashMap<Task, VMType>();
        HashMap<Task, Double> runtimes = new HashMap<Task, Double>();
        TopologicalOrder order = computeTopologicalOrder(dag, vmTypes, runtimes);
        /*
         * FIXME Later we will determine the best VM type for each task
         * assignEachTaskToCheapestResource()
         * criticalPath = computeCriticalPath()
         * while (criticalPath > deadline) {
         * upgradeTaskWithBestBangForBuck()
         * criticalPath = computeCriticalPath()
         * }
         */

        // Assign task deadlines to be the absolute latest finish time of task
        /*
         * HashMap<Task, Double> deadlines = new HashMap<Task, Double>();
         * for (Task t : order.reverse()) {
         * double deadline = getDeadline();
         * for (Task c : t.children) {
         * deadline = Math.min(deadline, deadlines.get(c)-runtimes.get(c));
         * }
         * deadlines.put(t, deadline);
         * }
         */

        // Use the fancy deadline distribution
        HashMap<Task, Double> deadlines = getDeadlineDistribution(order, runtimes, 0.7);

        // Create a new plan
        Plan plan = new Plan(currentPlan);

        // Actual finish times of tasks
        HashMap<Task, Double> finishTimes = new HashMap<Task, Double>();

        // Create queue
        HashSet<Task> queue = new HashSet<Task>();
        for (Task t : order) {
            if (t.getParents().size() == 0)
                queue.add(t);
        }

        while (queue.size() > 0) {

            // Best solution
            List<Solution> solutions = new LinkedList<Solution>();

            // Assign resources to each task
            for (Task t : queue) {
                double deadline = deadlines.get(t);
                double runtime = runtimes.get(t);
                VMType vmtype = vmTypes.get(t);

                // Compute earliest start time of task
                double earliestStart = 0.0;
                for (Task p : t.getParents()) {
                    earliestStart = Math.max(earliestStart, finishTimes.get(p));
                }

                // If earliest finish > deadline, then fail
                if (earliestStart + runtime >= deadline) {
                    throw new RuntimeException("Failed to schedule task");
                }

                // One option is to allocate a new resource
                Solution best = null;
                {
                    Resource r = new Resource(vmtype);
                    double cost = r.getCostWith(earliestStart, earliestStart + runtime);
                    Slot sl = new Slot(t, earliestStart, runtime);
                    best = new Solution(r, sl, cost, true);
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
                            Slot sl = new Slot(t, ast, runtime);
                            Solution soln = new Solution(r, sl, cost, false);
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
                            Slot sl = new Slot(t, ast, runtime);
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
                            Slot sl = new Slot(t, ast, runtime);
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
                            Slot sl = new Slot(t, ast, runtime);
                            Solution soln = new Solution(r, sl, cost, false);

                            if (soln.betterThan(best))
                                best = soln;

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
                        Slot sl = new Slot(t, ast, runtime);

                        Solution soln = new Solution(r, sl, cost, false);
                        if (soln.betterThan(best))
                            best = soln;
                    }
                }

                solutions.add(best);
            }

            // Find the optimal solution
            Solution optimal = null;
            for (Solution s : solutions) {
                optimal = bestSolution(s, optimal);
            }

            // Remove task from queue
            Task task = optimal.slot.task;
            queue.remove(task);

            // Schedule task on resource of best solution
            optimal.addToPlan(plan);

            // Reject plan right away
            double cost = plan.getCost();
            if (cost > getBudget()) {
                throw new NoFeasiblePlan("Exceeded budget: " + cost);
            }

            // Save actual finish time of task
            finishTimes.put(task, optimal.slot.start + runtimes.get(task));

            // Release any children of task
            for (Task c : task.getChildren()) {
                boolean ready = true;
                for (Task p : c.getParents()) {
                    if (!finishTimes.containsKey(p)) {
                        ready = false;
                        break;
                    }
                }
                if (ready) {
                    queue.add(c);
                }
            }
        }

        return plan;
    }

    /**
     * Return the cheaper solution (MinMin)
     */
    Solution bestSolution(Solution a, Solution b) {
        if (b == null)
            return a;

        if (a.cost < b.cost)
            return a;

        if (a.cost == b.cost && a.slot.start < b.slot.start)
            return a;

        return b;
    }
}
