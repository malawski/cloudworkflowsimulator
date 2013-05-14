package cws.core.storage;

import cws.core.cloudsim.CloudSimWrapper;
import cws.core.jobs.Job;

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
        notifyThatBeforeTransfersCompleted(job);
    }

    @Override
    public void onAfterTaskCompleted(Job job) {
        notifyThatAfterTransfersCompleted(job);
    }

    @Override
    public double getTransferTimeEstimation(Job job) {
        return 0.0; // instant transfer
    }
}
