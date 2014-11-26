package cws.core.dag;

import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.ImmutableList;

/**
 * @author Gideon Juve <juve@usc.edu>
 */
public class Task {
    /** Globally uniqe task id */
    private final String id;

    /** Transformation string taken from some daxes. Not really important and used only for logging. */
    private final String transformation;

    /** Number of MIPS needed to compute this task */
    private double size;

    /** Task's parents - the tasks that produce inputFiles */
    private final List<Task> parents = new ArrayList<Task>(2);

    /** Task's children - the tasks which this Task produce files for */
    private final List<Task> children = new ArrayList<Task>(5);

    /** Task's input files */
    private ImmutableList<DAGFile> inputFiles = ImmutableList.of();

    /** Task's output files */
    private ImmutableList<DAGFile> outputFiles = ImmutableList.of();

    public Task(String id, String transformation, double size) {
        this.id = id;
        this.transformation = transformation;
        this.size = size;
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
        return "<task id=" + getId() + ">";
    }

    public void scaleSize(double scalingFactor) {
        size *= scalingFactor;
    }

    public double getSize() {
        return size;
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

    public ImmutableList<DAGFile> getInputFiles() {
        return inputFiles;
    }

    public void addInputFiles(List<DAGFile> inputs) {
        this.inputFiles = ImmutableList.<DAGFile>builder().addAll(this.inputFiles).addAll(inputs).build();
    }

    public ImmutableList<DAGFile> getOutputFiles() {
        return outputFiles;
    }

    public void addOutputFiles(List<DAGFile> outputs) {
        this.outputFiles = ImmutableList.<DAGFile>builder().addAll(this.outputFiles).addAll(outputs).build();
    }
}
