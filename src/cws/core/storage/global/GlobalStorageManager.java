package cws.core.storage.global;

import cws.core.Job;
import cws.core.WorkflowEvent;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.storage.StorageManager;

/**
 * TODO(bryk): comment
 */
public class GlobalStorageManager extends StorageManager {
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
        super(cloudsim);
        this.readSpeed = readSpeed;
        this.writeSpeed = writeSpeed;
    }

    @Override
    public void onBeforeTaskStart(Job job) {
        getCloudsim().send(getId(), job.getVM().getId(), 0, WorkflowEvent.STORAGE_ALL_BEFORE_TRANSFERS_COMPLETED, job);
    }

    @Override
    public void onAfterTaskCompleted(Job job) {
        // TODO Auto-generated method stub

    }
}
