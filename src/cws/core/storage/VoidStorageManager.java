package cws.core.storage;

import cws.core.cloudsim.CloudSimWrapper;
import cws.core.dag.DAGFile;
import cws.core.dag.Task;
import cws.core.jobs.Job;

/**
 * Void storage manager that behaves as if the file transfers were indefinitely short. Using this manager effectively
 * means that transfers aren't taken into account.
 */
public class VoidStorageManager extends StorageManager {
    /** for generating unique transfer id */
    private int transferId = 0;

    public VoidStorageManager(CloudSimWrapper cloudsim) {
        super(cloudsim);
    }

    @Override
    public void onBeforeTaskStart(Job job) {
        for (DAGFile file : job.getTask().getInputFiles()) {
            statistics.addActualBytesRead(file.getSize());
            logInstantTransfer(job, file, "read");
        }
        statistics.addActualFilesRead(job.getTask().getInputFiles().size());
        notifyThatBeforeTransfersCompleted(job);
    }

    @Override
    public void onAfterTaskCompleted(Job job) {
        for (DAGFile file : job.getTask().getOutputFiles()) {
            logInstantTransfer(job, file, "write");
        }

        notifyThatAfterTransfersCompleted(job);
    }

    @Override
    public double getTransferTimeEstimation(Task task) {
        return 0.0; // instant transfer
    }

    /** We need somehow indicate (for validation scripts) that the transfer happened */
    private void logInstantTransfer(Job job, DAGFile file, String type) {
        String downloadMsg = String.format("Global %s transfer %d started: %s, size: %s, vm: %s, job_id: %d", type,
                transferId, file.getName(), file.getSize(), job.getVM().getId(), job.getID());
        String uploadMsg = String.format("Global %s transfer %d finished: %s, bytes transferred: %d, duration: %f",
                type, transferId, file.getName(), file.getSize(), 0.0);

        transferId++;

        getCloudsim().log(downloadMsg);
        getCloudsim().log(uploadMsg);
    }
}
