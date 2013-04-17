package cws.core.dag;

import static cws.core.WorkflowEvent.JOB_FINISHED;
import static cws.core.WorkflowEvent.JOB_STARTED;

import java.util.ArrayList;
import java.util.List;

import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.core.CloudSim;

import cws.core.Job;

/**
 * 
 * @author Gideon Juve <juve@usc.edu>
 */
public class Task {
    // TODO(bryk): document all these fields
    /** Globally uniqe task id */
    private String id = null;
    private String transformation = null;

    /** Number of MIPS needed to compute this task */
    private double size = 0.0;

    /** Task's parents - the tasks that produce inputFiles */
    private List<Task> parents = new ArrayList<Task>(2);

    /** Task's children - the tasks which this Task produce files for */
    private List<Task> children = new ArrayList<Task>(5);

    /** Task's input files */
    private List<String> inputFiles = null;

    /** Task's output files */
    private List<String> outputFiles = null;

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

    public List<String> getInputFiles() {
        return inputFiles;
    }

    public void setInputFiles(List<String> inputs) {
        this.inputFiles = inputs;
    }

    public List<String> getOutputFiles() {
        return outputFiles;
    }

    public void setOutputFiles(List<String> outputs) {
        this.outputFiles = outputs;
    }

    public void execute(Job job) {
        // Tell the owner
        CloudSim.send(job.getVM().getId(), job.getOwner(), 0.0, JOB_STARTED, job);

        // Compute the duration of the job on this VM
        double size = getSize();
        double predictedRuntime = size / job.getVM().getMIPS();

        // Compute actual runtime
        double actualRuntime = job.getVM().getRuntimeDistribution().getActualRuntime(predictedRuntime);

        // Decide whether the job succeeded or failed
        if (job.getVM().getFailureModel().failureOccurred()) {
            job.setResult(Job.Result.FAILURE);

            // How long did it take to fail?
            actualRuntime = job.getVM().getFailureModel().runtimeBeforeFailure(actualRuntime);
        } else {
            job.setResult(Job.Result.SUCCESS);
        }

        CloudSim.send(job.getVM().getId(), job.getVM().getId(), actualRuntime, JOB_FINISHED, job);
        Log.printLine(CloudSim.clock() + " Starting job " + job.getID() + " on VM " + job.getVM().getId()
                + " duration " + actualRuntime);
    }
}
