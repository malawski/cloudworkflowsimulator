package cws.core.storage.global;

import static cws.core.WorkflowEvent.GLOBAL_STORAGE_READ_FINISHED;
import static cws.core.WorkflowEvent.GLOBAL_STORAGE_START_READ;
import static cws.core.WorkflowEvent.GLOBAL_STORAGE_START_WRITE;
import static cws.core.WorkflowEvent.GLOBAL_STORAGE_UPDATE_READ_PROGRESS;
import static cws.core.WorkflowEvent.GLOBAL_STORAGE_UPDATE_WRITE_PROGRESS;
import static cws.core.WorkflowEvent.GLOBAL_STORAGE_WRITE_FINISHED;
import cws.core.cloudsim.CWSSimEntity;
import cws.core.cloudsim.CWSSimEvent;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.exception.UnknownWorkflowEventException;

/**
 * TODO(bryk): comment
 */
public class GlobalStorageManager extends CWSSimEntity {
    /**
     * Average read speed of the storage.
     * TODO(bryk): randomize under some distribution r/w speeds.
     */
    private double readSpeed;

    /**
     * Average write speed of the storage.
     * TODO(bryk): randomize under some distribution r/w speeds.
     */
    private double writeSpeed;

    /**
     * Initializes GlobalStorageManager with the appropriate parameters. Check their documentation for more information.
     */
    public GlobalStorageManager(double readSpeed, double writeSpeed, CloudSimWrapper cloudsim) {
        super("GlobalStorageManager", cloudsim);
        this.readSpeed = readSpeed;
        this.writeSpeed = writeSpeed;
    }

    /**
     * @see CWSSimEntity#processEvent(CWSSimEvent)
     */
    @Override
    public void processEvent(CWSSimEvent ev) {
        switch (ev.getTag()) {
        case GLOBAL_STORAGE_START_READ:
            onStartRead((GlobalStorageRead) ev.getData());
            break;
        case GLOBAL_STORAGE_START_WRITE:
            onStartWrite((GlobalStorageWrite) ev.getData());
            break;
        case GLOBAL_STORAGE_UPDATE_WRITE_PROGRESS:
            onUpdateWriteProgress((GlobalStorageWrite) ev.getData());
            break;
        case GLOBAL_STORAGE_UPDATE_READ_PROGRESS:
            onUpdateReadProgress((GlobalStorageRead) ev.getData());
            break;
        case GLOBAL_STORAGE_READ_FINISHED:
            onReadFinished((GlobalStorageRead) ev.getData());
            break;
        case GLOBAL_STORAGE_WRITE_FINISHED:
            onWriteFinished((GlobalStorageWrite) ev.getData());
            break;
        default:
            throw new UnknownWorkflowEventException("GlobalStorageManager could not handle the event: " + ev);
        }
    }

    /**
     * TODO(bryk): implement and document it
     * @param data
     */
    private void onWriteFinished(GlobalStorageWrite data) {
    }

    /**
     * TODO(bryk): implement and document it
     * @param data
     */
    private void onReadFinished(GlobalStorageRead data) {
    }

    /**
     * TODO(bryk): implement and document it
     * @param data
     */
    private void onUpdateReadProgress(GlobalStorageRead data) {
    }

    /**
     * TODO(bryk): implement and document it
     * @param data
     */
    private void onUpdateWriteProgress(GlobalStorageWrite data) {
    }

    /**
     * TODO(bryk): implement and document it
     * @param data
     */
    private void onStartWrite(GlobalStorageWrite data) {
    }

    /**
     * TODO(bryk): implement and document it
     * @param data
     */
    private void onStartRead(GlobalStorageRead data) {
    }

    @Override
    public void shutdownEntity() {
        // do nothing
    }

    @Override
    public void startEntity() {
        // do nothing
    }
}
