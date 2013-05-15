package cws.core.storage.global;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import cws.core.WorkflowEvent;
import cws.core.cloudsim.CWSSimEvent;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.dag.DAGFile;
import cws.core.jobs.Job;
import cws.core.storage.StorageManager;

/**
 * TODO(bryk): comment
 * TODO(bryk): randomize parameters under some distribution
 */
public class GlobalStorageManager extends StorageManager {
    /** Map of jobs' active reads - the ones that progress at any given moment */
    private Map<Job, List<GlobalStorageTransfer>> reads = new HashMap<Job, List<GlobalStorageTransfer>>();

    /** Map of jobs' active writes - the ones that progress at any given moment */
    private Map<Job, List<GlobalStorageTransfer>> writes = new HashMap<Job, List<GlobalStorageTransfer>>();

    /** Set of parameters for this storage */
    private GlobalStorageParams params;

    /**
     * Initializes GlobalStorageManager with the appropriate parameters. Check their documentation for more information.
     */
    public GlobalStorageManager(GlobalStorageParams params, CloudSimWrapper cloudsim) {
        super(cloudsim);
        this.params = params;
    }

    /**
     * 1. If the job has no input files the method finishes immediately.
     * 2. Else it creates transfer for each input file. The transfers are then handled by the event system.
     * 
     * @see StorageManager#onBeforeTaskStart(Job)
     */
    @Override
    protected void onBeforeTaskStart(Job job) {
        List<DAGFile> files = job.getTask().getInputFiles();
        if (files.size() == 0) {
            notifyThatBeforeTransfersCompleted(job);
        } else {
            List<GlobalStorageTransfer> jobTransfers = new ArrayList<GlobalStorageTransfer>();
            reads.put(job, jobTransfers);
            for (DAGFile file : files) {
                GlobalStorageTransfer read = new GlobalStorageTransfer(job, file);
                jobTransfers.add(read);
                double transferTime = file.getSize() / params.getReadSpeed();
                getCloudsim().log(
                        "Global transfer started: " + read.getFile().getName() + ", bytes transferred: "
                                + read.getFile().getSize() + ", duration: " + read.getDuration());
                getCloudsim().send(getId(), getId(), transferTime, WorkflowEvent.GLOBAL_STORAGE_READ_FINISHED, read);
            }
        }
    }

    /**
     * 1. If the job has no output files the method finishes immediately.
     * 2. Else it creates transfer for each output file. The transfers are then handled by the event system.
     * 
     * @see StorageManager#onAfterTaskCompleted(Job)
     */
    @Override
    protected void onAfterTaskCompleted(Job job) {
        List<DAGFile> files = job.getTask().getOutputFiles();
        if (files.size() == 0) {
            notifyThatAfterTransfersCompleted(job);
        } else {
            List<GlobalStorageTransfer> jobTransfers = new ArrayList<GlobalStorageTransfer>();
            writes.put(job, jobTransfers);
            for (DAGFile file : files) {
                GlobalStorageTransfer write = new GlobalStorageTransfer(job, file);
                jobTransfers.add(write);
                double transferTime = file.getSize() / params.getWriteSpeed();
                getCloudsim().log(
                        "Global transfer started: " + write.getFile().getName() + ", bytes transferred: "
                                + write.getFile().getSize() + ", duration: " + write.getDuration());
                getCloudsim().send(getId(), getId(), transferTime, WorkflowEvent.GLOBAL_STORAGE_WRITE_FINISHED, write);
            }
        }
    }

    /**
     * Called after a write has finished. Logs message. If all writes have completed then notifies appropriate VM.
     */
    private void onWriteFinished(GlobalStorageTransfer write) {
        if (onTransferFinished(write, writes)) {
            notifyThatAfterTransfersCompleted(write.getJob());
        }
    }

    /**
     * Called after a read has finished. Logs message. If all reads have completed then notifies appropriate VM.
     */
    private void onReadFinished(GlobalStorageTransfer read) {
        if (onTransferFinished(read, reads)) {
            notifyThatBeforeTransfersCompleted(read.getJob());
        }
    }

    /**
     * Cleans up after transfer's finish.
     * @param transfer - the transfer that has finished
     * @param transfers - map with active transfers this transfer belongs to (e.g. writes or reads)
     * @return true if this was the last transfer in the job, false otherwise
     */
    private boolean onTransferFinished(GlobalStorageTransfer transfer, Map<Job, List<GlobalStorageTransfer>> transfers) {
        getCloudsim().log(
                "Global transfer finished: " + transfer.getFile().getName() + ", bytes transferred: "
                        + transfer.getFile().getSize() + ", duration: " + transfer.getDuration());
        List<GlobalStorageTransfer> jobTransfers = transfers.get(transfer.getJob());
        jobTransfers.remove(transfer);
        if (jobTransfers.isEmpty()) {
            transfers.remove(transfer.getJob());
            return true;
        } else {
            return false;
        }
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
     * Trivial transfer estimation based o read and write speeds. This seems good enough, but we might change the
     * implementation in the future
     * 
     * @see StorageManager#getTransferTimeEstimation(Job)
     */
    @Override
    public double getTransferTimeEstimation(Job job) {
        double time = 0.0;
        for (DAGFile file : job.getTask().getInputFiles()) {
            time += file.getSize() / params.getReadSpeed();
        }
        for (DAGFile file : job.getTask().getOutputFiles()) {
            time += file.getSize() / params.getWriteSpeed();
        }
        return time;
    }
}
