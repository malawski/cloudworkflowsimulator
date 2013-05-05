package cws.core.storage;

import org.cloudbus.cloudsim.core.SimEntity;
import org.cloudbus.cloudsim.core.SimEvent;

import cws.core.jobs.Job;
import cws.core.WorkflowEvent;
import cws.core.cloudsim.CWSSimEntity;
import cws.core.cloudsim.CWSSimEvent;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.exception.UnknownWorkflowEventException;

/**
 * TODO(bryk): comment
 */
public abstract class StorageManager extends CWSSimEntity implements WorkflowEvent {
    /**
     * Creates new object so that every StorageManager implementation will have the same name.
     */
    public StorageManager(CloudSimWrapper cloudsim) {
        super("StorageManager", cloudsim);
    }

    /**
     * Called just before a VM starts a job. You should get here job's input files to the VM.
     * @param job - the job that owns the task that is going to start
     */
    public abstract void onBeforeTaskStart(Job job);

    /**
     * Called just after a job has finished. You should here transfer out job's output files somewhere or register their
     * names (it's up to the particular implementation).
     * @param job - the job that owns the task that has completed
     */
    public abstract void onAfterTaskCompleted(Job job);

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
    public void notifyThatBeforeTransfersCompleted(Job job) {
        getCloudsim().send(getId(), job.getVM().getId(), 0, WorkflowEvent.STORAGE_ALL_BEFORE_TRANSFERS_COMPLETED, job);
    }

    /**
     * Notifies parent VM that all output transfers have completed and thus the job can be finished.
     * 
     * @param job - the job for which all output transfers have completed
     */
    public void notifyThatAfterTransfersCompleted(Job job) {
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
