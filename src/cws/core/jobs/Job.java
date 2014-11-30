package cws.core.jobs;

import com.google.common.base.Preconditions;

import cws.core.VM;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.dag.DAGJob;
import cws.core.dag.Task;

/**
 * A Job is a unit of work that executes on a VM
 * 
 * @author Gideon Juve <juve@usc.edu>
 */
public class Job {
    private static int next_id = 0;

    /** Job states */
    public static enum State {
        QUEUED, IDLE, RUNNING, TERMINATED
    }

    /** Job results */
    public static enum Result {
        NONE, SUCCESS, FAILURE
    }

    /** The ID of this job */
    private final int id;

    /** The VM where this job ran */
    private VM vm;

    /** The DAG that spawned this job */
    private final DAGJob dagJob;

    /** The task that this job executes */
    private final Task task;

    /** The owner of the job */
    private final int owner;

    /** Time the job was released */
    private final double releaseTime;

    /** Submit time of the job */
    private double submitTime;

    /** The start time of the job */
    private double startTime;

    /** The finish time of the job */
    private double finishTime;

    /** What is the current state of the job? */
    private State state;

    /** Job result */
    private Result result;

    /** If this job is a retry of the task */
    private boolean isRetry = false;

    public Job(DAGJob dagJob, Task task, int owner, CloudSimWrapper cloudsim) {
        Preconditions.checkNotNull(dagJob);
        Preconditions.checkNotNull(task);
        this.id = next_id++;
        this.releaseTime = cloudsim.clock();
        this.state = State.QUEUED;
        this.result = Result.NONE;
        this.dagJob = dagJob;
        this.task = task;
        this.owner = owner;
    }

    public int getID() {
        return id;
    }

    public int getOwner() {
        return owner;
    }

    public void setVM(VM vm) {
        this.vm = vm;
    }

    public VM getVM() {
        return vm;
    }

    public DAGJob getDAGJob() {
        return dagJob;
    }

    public Task getTask() {
        return task;
    }

    public double getReleaseTime() {
        return this.releaseTime;
    }

    public void setSubmitTime(double submitTime) {
        this.submitTime = submitTime;
    }

    public double getSubmitTime() {
        return submitTime;
    }

    public void setStartTime(double startTime) {
        this.startTime = startTime;
    }

    public double getStartTime() {
        return startTime;
    }

    public void setFinishTime(double finishTime) {
        this.finishTime = finishTime;
    }

    public double getFinishTime() {
        return finishTime;
    }

    public double getDuration() {
        return finishTime - startTime;
    }

    public void setState(State state) {
        this.state = state;
    }

    public State getState() {
        return state;
    }

    public void setResult(Result result) {
        this.result = result;
    }

    public Result getResult() {
        return result;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + id;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Job other = (Job) obj;
        if (id != other.id)
            return false;
        return true;
    }

    @Override
    public String toString() {
        return String.format("Job %s (task_id = %s, workflow = %s)", getID(), getTask().getId(), getDAGJob().getDAG()
                .getId());
    }

    public boolean isRetry() {
        return isRetry;
    }

    public void setRetry(boolean retry) {
        isRetry = retry;
    }
}
