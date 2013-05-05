package cws.core.storage.global;

import java.util.List;

import cws.core.cloudsim.CloudSimWrapper;
import cws.core.jobs.Job;
import cws.core.storage.StorageManager;

/**
 * TODO(bryk): comment
 * TODO(bryk): randomize parameters under some distribution
 */
public class GlobalStorageManager extends StorageManager {
    /**
     * Average read speed of the storage.
     */
    private double readSpeed;

    /**
     * Average write speed of the storage.
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
        List<String> files = job.getTask().getInputFiles();
        if (files.size() == 0) {
            notifyThatBeforeTransfersCompleted(job);
        } else {
            // TODO(bryk): implement
        }
    }

    @Override
    public void onAfterTaskCompleted(Job job) {
        List<String> files = job.getTask().getOutputFiles();
        if (files.size() == 0) {
            notifyThatAfterTransfersCompleted(job);
        } else {
            // TODO(bryk): implement
        }
    }
}
