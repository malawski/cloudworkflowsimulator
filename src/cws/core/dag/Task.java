package cws.core.dag;

import java.util.ArrayList;
import java.util.List;

import cws.core.Job;

/**
 * @author Gideon Juve <juve@usc.edu>
 */
public abstract class Task {
    // TODO(bryk): document all these fields
    /** Globally uniqe task id */
    private String id = null;
    /** Task's parents - the tasks that produce inputFiles */
    private List<ComputationTask> parents = new ArrayList<ComputationTask>(2);

    /** Task's children - the tasks which this Task produce files for */
    private List<ComputationTask> children = new ArrayList<ComputationTask>(5);

    public Task(String id) {
        this.id = id;
    }

    public abstract void execute(Job job);

    /**
     * IT IS IMPORTANT THAT THESE ARE NOT IMPLEMENTED
     * 
     * Using the default implementation allows us to put Tasks from
     * different DAGs that have the same task ID into a single HashMap
     * or HashSet. That way we don't have to maintain a reference from
     * the Task to the DAG that owns it--we can mix tasks from different
     * DAGs in the same data structure.
     * 
     * <pre>
     * public int hashCode() {
     *     return id.hashCode();
     * }
     * 
     * public boolean equals(Object o) {
     *     if (!(o instanceof Task)) {
     *         return false;
     *     }
     *     Task t = (Task) o;
     *     return this.id.equals(t.id);
     * }
     * </pre>
     */

    @Override
    public String toString() {
        return "<Task id=" + getId() + ">";
    }

    public String getId() {
        return id;
    }

    public List<ComputationTask> getParents() {
        return parents;
    }

    public List<ComputationTask> getChildren() {
        return children;
    }
}
