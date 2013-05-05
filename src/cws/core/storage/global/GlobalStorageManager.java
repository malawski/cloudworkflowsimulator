package cws.core.storage.global;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import cws.core.WorkflowEvent;
import cws.core.cloudsim.CWSSimEvent;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.jobs.Job;
import cws.core.storage.StorageManager;

/**
 * TODO(bryk): comment
 * TODO(bryk): randomize parameters under some distribution
 */
public class GlobalStorageManager extends StorageManager {
    private Map<Job, List<GlobalStorageTransfer>> reads = new HashMap<Job, List<GlobalStorageTransfer>>();
    private Map<Job, List<GlobalStorageTransfer>> writes = new HashMap<Job, List<GlobalStorageTransfer>>();

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
    protected void onUnknownSimEvent(CWSSimEvent ev) {
        switch (ev.getTag()) {
        case WorkflowEvent.GLOBAL_STORAGE_READ_FINISHED:
            onReadFinished((GlobalStorageTransfer) ev.getData());
            break;
        case WorkflowEvent.GLOBAL_STORAGE_WRITE_FINISHED:
            onWriteFinished((GlobalStorageTransfer) ev.getData());
            break;
        default:
            super.onUnknownSimEvent(ev);
            break;
        }
    }

    /**
     * Called after a write has finished. Logs message. If all writes have completed then notifies appropriate VM.
     */
    private void onWriteFinished(GlobalStorageTransfer write) {
        getCloudsim().log(
                "Global storage write has finished: " + write.getName() + ", bytes transferred: " + write.getSize()
                        + ", duration: " + write.getDuration());
        List<GlobalStorageTransfer> jobTransfers = writes.get(write.getJob());
        jobTransfers.remove(write);
        if (jobTransfers.isEmpty()) {
            reads.remove(write.getJob());
            notifyThatAfterTransfersCompleted(write.getJob());
        }
    }

    /**
     * Called after a read has finished. Logs message. If all reads have completed then notifies appropriate VM.
     */
    private void onReadFinished(GlobalStorageTransfer read) {
        getCloudsim().log(
                "Global storage red has finished: " + read.getName() + ", bytes transferred: " + read.getSize()
                        + ", duration: " + read.getDuration());
        List<GlobalStorageTransfer> jobTransfers = reads.get(read.getJob());
        jobTransfers.remove(read);
        if (jobTransfers.isEmpty()) {
            reads.remove(read.getJob());
            notifyThatBeforeTransfersCompleted(read.getJob());
        }
    }

    @Override
    public void onBeforeTaskStart(Job job) {
        List<String> files = job.getTask().getInputFiles();
        if (files.size() == 0) {
            notifyThatBeforeTransfersCompleted(job);
        } else {
            // TODO(bryk): remove this hard coded value
            long size = 1000;
            List<GlobalStorageTransfer> jobTransfers = new ArrayList<GlobalStorageTransfer>();
            reads.put(job, jobTransfers);
            for (String fileName : files) {
                GlobalStorageTransfer read = new GlobalStorageTransfer(job, fileName, size);
                jobTransfers.add(read);
                double transferTime = size / readSpeed;
                System.out.println("TRANS" + transferTime);
                getCloudsim().send(getId(), getId(), transferTime, WorkflowEvent.GLOBAL_STORAGE_READ_FINISHED, read);
            }
        }
    }

    @Override
    public void onAfterTaskCompleted(Job job) {
        List<String> files = job.getTask().getOutputFiles();
        if (files.size() == 0) {
            notifyThatAfterTransfersCompleted(job);
        } else {
            // TODO(bryk): remove this hard coded value
            long size = 1000;
            List<GlobalStorageTransfer> jobTransfers = new ArrayList<GlobalStorageTransfer>();
            writes.put(job, jobTransfers);
            for (String fileName : files) {
                GlobalStorageTransfer write = new GlobalStorageTransfer(job, fileName, size);
                jobTransfers.add(write);
                double transferTime = size / writeSpeed;
                getCloudsim().send(getId(), getId(), transferTime, WorkflowEvent.GLOBAL_STORAGE_WRITE_FINISHED, write);
            }
        }
    }
}
