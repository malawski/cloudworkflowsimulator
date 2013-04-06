package cws.core.storage;

import org.cloudbus.cloudsim.core.SimEntity;
import org.cloudbus.cloudsim.core.SimEvent;

import cws.core.Job;
import cws.core.WorkflowEvent;
import cws.core.exception.UnknownWorkflowEventException;

/**
 * TODO(bryk): comment
 */
public abstract class StorageManager extends SimEntity implements WorkflowEvent {
    /**
     * Creates new object so that every StorageManager implementation will have the same name.
     */
    public StorageManager() {
        super("StorageManager");
    }

    /**
     * Called when a file transfer is completed. Typically you'd want here to inform host VM that the transfer is
     * completed.
     * @param fileTransfer - the transfer that has just completed
     */
    public abstract void onFileTransferCompleted(FileTransfer fileTransfer);

    /**
     * Called just before a VM starts a job. You should here get input files to the VM.
     * @param job - the job that is going to start
     */
    public abstract void onBeforeJobStart(Job job);

    /**
     * Called just after a job has finished. You should here transfer out output files somewhere or register their names
     * (it's up to the particular implementation).
     * @param job
     */
    public abstract void onAfterJobFinish(Job job);

    /**
     * @see SimEntity#processEvent(SimEvent)
     */
    @Override
    public void processEvent(SimEvent ev) {
        switch (ev.getTag()) {
        case FILE_MANAGER_BEFORE_JOB_START:
            onBeforeJobStart((Job) ev.getData());
            break;
        case FILE_MANAGER_AFTER_JOB_FINISH:
            onAfterJobFinish((Job) ev.getData());
            break;
        case FILE_TRANSFER_COMPLETED:
            onFileTransferCompleted((FileTransfer) ev.getData());
            break;
        default:
            onUnknownSimEvent(ev);
            break;
        }
    }

    /**
     * Called on unknown event in {@link #processEvent(SimEvent)}
     * @param ev - the unknown event which occured.
     */
    protected void onUnknownSimEvent(SimEvent ev) {
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
