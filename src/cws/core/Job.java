package cws.core;

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
        NEW,
        QUEUED,
        RUNNING,
        SUCCESS,
        FAILURE
    }
    
    /** The ID of this job */
    private int id;
    
    /** The VM where this job ran */
    private VM vm;
    
    /** The task that this job executes */
    private Task task;
    
    /** The owner of the job */
    private int owner;
    
    /** The size of the job in millions of instructions (MI) */
    private int size;
    
    /** The time the job was queued on the remote resource */
    private double remoteQueueTime;
    
    /** The start time of the job */
    private double startTime;
    
    /** The finish time of the job */
    private double finishTime;
    
    /** Did the job succeed or fail? */
    private State state;
    
    public Job(int size) {
        this.id = next_id++;
        this.size = size;
        this.state = State.NEW;
    }
    
    public int getID() {
        return id;
    }
    
    public void setOwner(int owner) {
        this.owner = owner;
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
    
    public void setTask(Task task) {
        this.task = task;
    }
    
    public Task getTask() {
        return task;
    }
    
    public void setSize(int size) {
        this.size = size;
    }
    
    public int getSize() {
        return size;
    }
    
    public void setRemoteQueueTime(double remoteQueueTime) {
        this.remoteQueueTime = remoteQueueTime;
    }
    
    public double getRemoteQueueTime() {
        return remoteQueueTime;
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
}
