package cws.core.storage;

import org.cloudbus.cloudsim.core.SimEntity;
import org.cloudbus.cloudsim.core.SimEvent;

import cws.core.WorkflowEvent;
import cws.core.cloudsim.CWSSimEntity;
import cws.core.cloudsim.CWSSimEvent;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.dag.DAGFile;
import cws.core.dag.Task;
import cws.core.exception.UnknownWorkflowEventException;
import cws.core.jobs.Job;

/**
 * Abstract class for all storage managers. It should be subclassed and implemented.
 * 
 * The basic idea behind every StorageManager is that receives STORAGE_BEFORE_TASK_START and
 * STORAGE_AFTER_TASK_COMPLETED events with a Job specified. After that it transfers all the files and eventually sends
 * back STORAGE_ALL_BEFORE_TRANSFERS_COMPLETED and STORAGE_ALL_AFTER_TRANSFERS_COMPLETED events.
 */
public abstract class StorageManager extends CWSSimEntity implements WorkflowEvent {
    /**
     * Creates new object so that every StorageManager implementation will have the same name.
     */
    public StorageManager(CloudSimWrapper cloudsim) {
        super("StorageManager", cloudsim);
    }

    public abstract boolean isInCache(DAGFile file, Job job);

    /**
     * Estimates the sum of all transfers for the given job. Note that the estimations don't need to be 100% accurate.
     * @param task - the task to estimate transfers for
     */
    public abstract double getTransferTimeEstimation(Task task);

    /**
     * Called just before a VM starts a job. You should get here job's input files to the VM.
     * @param job - the job that owns the task that is going to start
     */
    protected abstract void onBeforeTaskStart(Job job);

    /**
     * Called just after a job has finished. You should here transfer out job's output files somewhere or register their
     * names (it's up to the particular implementation).
     * @param job - the job that owns the task that has completed
     */
    protected abstract void onAfterTaskCompleted(Job job);

    /**
     * @see SimEntity#processEvent(SimEvent)
     */
    @Override
    public void processEvent(CWSSimEvent ev) {
        switch (ev.getTag()) {
        case WorkflowEvent.STORAGE_BEFORE_TASK_START:
            onBeforeTaskStart((Job) ev.getData());
            break;
        case WorkflowEvent.STORAGE_AFTER_TASK_COMPLETED:
            onAfterTaskCompleted((Job) ev.getData());
            break;
        default:
            onUnknownSimEvent(ev);
            break;
        }
    }

    /**
     * Notifies parent VM that all input transfers have completed and thus the job can be started.
     * 
     * @param job - the job for which all input transfers have completed
     */
    protected void notifyThatBeforeTransfersCompleted(Job job) {
        getCloudsim().send(getId(), job.getVM().getId(), 0, WorkflowEvent.STORAGE_ALL_BEFORE_TRANSFERS_COMPLETED, job);
    }

    /**
     * Notifies parent VM that all output transfers have completed and thus the job can be finished.
     * 
     * @param job - the job for which all output transfers have completed
     */
    protected void notifyThatAfterTransfersCompleted(Job job) {
        getCloudsim().send(getId(), job.getVM().getId(), 0, WorkflowEvent.STORAGE_ALL_AFTER_TRANSFERS_COMPLETED, job);
    }

    /**
     * Called on unknown event occurred in {@link #processEvent(SimEvent)}
     * @param ev - the unknown event which occurred.
     */
    protected void onUnknownSimEvent(CWSSimEvent ev) {
        throw new UnknownWorkflowEventException("Unknown event in StorageManager: " + ev);
    }

    /**
     * @see SimEntity#startEntity()
     */
    @Override
    public void startEntity() {
        // do nothing
    }

    /**
     * @see SimEntity#shutdownEntity()
     */
    @Override
    public void shutdownEntity() {
        // do nothing
    }
}
