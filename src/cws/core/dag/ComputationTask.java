package cws.core.dag;

import static cws.core.WorkflowEvent.JOB_FINISHED;

import java.util.List;

import org.cloudbus.cloudsim.core.CloudSim;

import cws.core.Job;

/**
 * A computation task. It is usually a workflow task from DAG or DAX file. It has input and output files and its
 * execution time is based on the executing VM speed.
 */
public class ComputationTask extends Task {
    /** TODO(bryk): Ask @malawski what is this */
    private String transformation = null;
    /** Task's input files */
    private List<String> inputFiles = null;
    /** Task's output files */
    private List<String> outputFiles = null;
    /** Number of MIPS needed to compute this task */
    private double size = 0.0;

    /**
     * @param id - globally unique task name
     * @param transformation - TODO(bryk): ask @malawski what is this
     * @param size - number of MIPS needed to compute this task
     */
    public ComputationTask(String id, String transformation, double size) {
        super(id);
        this.transformation = transformation;
        this.setSize(size);

        // SIPHT workflows have tasks with 0.0 size so we commented out this condition
        // if (size <= 0) {
        // throw new RuntimeException(
        // "Invalid size for task "+id+" ("+transformation+"): "+size);
        // }
    }

    @Override
    public void execute(Job job) {
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
    }

    public String getTransformation() {
        return transformation;
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

    public double getSize() {
        return size;
    }

    public void setSize(double size) {
        this.size = size;
    }
}
