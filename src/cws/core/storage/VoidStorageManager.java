package cws.core.storage;

import cws.core.jobs.Job;
import cws.core.WorkflowEvent;
import cws.core.cloudsim.CloudSimWrapper;

/**
 * Void storage manager that behaves as if the file transfers were indefinitely short. Using this manager effectively
 * means that transfers aren't taken into account.
 */
public class VoidStorageManager extends StorageManager {
    public VoidStorageManager(CloudSimWrapper cloudsim) {
        super(cloudsim);
    }

    @Override
    public void onBeforeTaskStart(Job job) {
        getCloudsim().send(getId(), job.getVM().getId(), 0, WorkflowEvent.STORAGE_ALL_BEFORE_TRANSFERS_COMPLETED, job);
    }

    @Override
    public void onAfterTaskCompleted(Job job) {
        getCloudsim().send(getId(), job.getVM().getId(), 0, WorkflowEvent.STORAGE_ALL_AFTER_TRANSFERS_COMPLETED, job);
    }
}
