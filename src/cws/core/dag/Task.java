package cws.core.dag;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Gideon Juve <juve@usc.edu>
 */
public class Task {
    /** Globally uniqe task id */
    private String id = null;

    /** Transformation string taken from some daxes. Not really important and used only for logging. */
    private String transformation = null;

    /** Number of MIPS needed to compute this task */
    private double size = 0.0;

    /** Task's parents - the tasks that produce inputFiles */
    private List<Task> parents = new ArrayList<Task>(2);

    /** Task's children - the tasks which this Task produce files for */
    private List<Task> children = new ArrayList<Task>(5);

    /** Task's input files */
    private List<DAGFile> inputFiles = new ArrayList<DAGFile>();

    /** Task's output files */
    private List<DAGFile> outputFiles = new ArrayList<DAGFile>();

    public Task(String id, String transformation, double size) {
        this.id = id;
        this.transformation = transformation;
        this.setSize(size);
        // SIPHT workflows have tasks with 0.0 size so we commented out this condition
        // if (size <= 0) {
        // throw new RuntimeException(
        // "Invalid size for task "+id+" ("+transformation+"): "+size);
        // }
    }

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

    public double getSize() {
        return size;
    }

    public void setSize(double size) {
        this.size = size;
    }

    public String getTransformation() {
        return transformation;
    }

    public String getId() {
        return id;
    }

    public List<Task> getParents() {
        return parents;
    }

    public List<Task> getChildren() {
        return children;
    }

    public List<DAGFile> getInputFiles() {
        return inputFiles;
    }

    public void addInputFiles(List<DAGFile> inputs) {
        this.inputFiles.addAll(inputs);
    }

    public List<DAGFile> getOutputFiles() {
        return outputFiles;
    }

    public void addOutputFiles(List<DAGFile> outputs) {
        this.outputFiles.addAll(outputs);
    }
}
