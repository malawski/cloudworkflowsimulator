package cws.core.storage.global;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import cws.core.WorkflowEvent;
import cws.core.cloudsim.CWSSimEvent;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.dag.DAGFile;
import cws.core.dag.Task;
import cws.core.jobs.Job;
import cws.core.storage.StorageManager;
import cws.core.storage.cache.VMCacheManager;

/**
 * Manager which stores files on a global storage. This should loosely resemble Amazon's S3 storage.<br>
 * 
 * GlobalStorageManager uses {@link VMCacheManager} for caching.
 * 
 * TODO(bryk): randomize parameters under some distribution
 */
public class GlobalStorageManager extends StorageManager {
    /** Map of jobs' active reads - the ones that progress at any given moment */
    private Map<Job, List<GlobalStorageTransfer>> reads = new HashMap<Job, List<GlobalStorageTransfer>>();

    /** Map of jobs' active writes - the ones that progress at any given moment */
    private Map<Job, List<GlobalStorageTransfer>> writes = new HashMap<Job, List<GlobalStorageTransfer>>();

    /** A set of parameters for this storage */
    private GlobalStorageParams params;

    /** A set of parameters used to simulate congestion */
    private CongestedGlobalStorageParams congestedParams;

    /** Cache manager used by this storage */
    private VMCacheManager cacheManager;

    /**
     * Initializes GlobalStorageManager with the appropriate parameters. Check their documentation for more information.
     */
    public GlobalStorageManager(GlobalStorageParams params, VMCacheManager cacheManager, CloudSimWrapper cloudsim) {
        super(cloudsim);
        this.params = params;
        this.cacheManager = cacheManager;
        this.congestedParams = new CongestedGlobalStorageParams(params);
    }

    /**
     * 1. If the job has no input files the method finishes immediately.
     * 2. Else it creates transfer for each input file. The transfers are then handled by the event system.
     * 
     * @see StorageManager#onBeforeTaskStart(Job)
     */
    @Override
    protected void onBeforeTaskStart(Job job) {
        List<DAGFile> notCachedFiles = new ArrayList<DAGFile>();
        for (DAGFile file : job.getTask().getInputFiles()) {
            if (!cacheManager.getFileFromCache(file, job)) {
                notCachedFiles.add(file);
            }
        }
        if (notCachedFiles.size() == 0) {
            notifyThatBeforeTransfersCompleted(job);
        } else {
            startTransfers(notCachedFiles, job, reads, WorkflowEvent.GLOBAL_STORAGE_READ_PROGRESS, "read");
            congestedParams.addReads(notCachedFiles.size());
            updateSpeedCongestion();
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
            startTransfers(files, job, writes, WorkflowEvent.GLOBAL_STORAGE_WRITE_PROGRESS, "write");
            congestedParams.addWrites(files.size());
            updateSpeedCongestion();
        }
    }

    /**
     * Starts transfers for the given job.
     * @param files - the files to start transfers for.
     * @param job - the job that starts the transfers.
     * @param transfers - the map with active transfers this transfer belongs to (e.g. writes or reads).
     * @param progressEvent - the event that will be sent upon transfer start.
     * @param transferType - the type of this transfer, e.g. "write".
     */
    private void startTransfers(List<DAGFile> files, Job job, Map<Job, List<GlobalStorageTransfer>> transfers,
            int progressEvent, String transferType) {
        List<GlobalStorageTransfer> jobTransfers = new ArrayList<GlobalStorageTransfer>();
        transfers.put(job, jobTransfers);
        for (DAGFile file : files) {
            GlobalStorageTransfer write = new GlobalStorageTransfer(job, file);
            jobTransfers.add(write);
            String logMsg = String.format("Global %s transfer %s started: %s, size: %s, vm: %s, job_id: %d",
                    transferType, write.getId(), write.getFile().getName(), write.getFile().getSize(), job.getVM()
                            .getId(), job.getID());
            getCloudsim().log(logMsg);
            getCloudsim().send(getId(), getId(), params.getLatency(), progressEvent, write);
        }
    }

    /**
     * Called after a write has finished. Logs message. If all writes have completed then notifies appropriate VM.
     */
    private void onWriteFinished(GlobalStorageTransfer write) {
        if (onTransferFinished(write, writes, "write")) {
            notifyThatAfterTransfersCompleted(write.getJob());
        }
        cacheManager.putFileToCache(write.getFile(), write.getJob());
        congestedParams.removeWrites(1);
        updateSpeedCongestion();
    }

    /**
     * Called after a read has finished. Logs message. If all reads have completed then notifies appropriate VM.
     */
    private void onReadFinished(GlobalStorageTransfer read) {
        if (onTransferFinished(read, reads, "read")) {
            notifyThatBeforeTransfersCompleted(read.getJob());
        }
        cacheManager.putFileToCache(read.getFile(), read.getJob());
        congestedParams.removeReads(1);
        updateSpeedCongestion();
        statistics.addActualBytesRead(read.getFile().getSize());
        statistics.addActualFilesRead(1);
    }

    /**
     * Cleans up after transfer's finish.
     * @param transfer - the transfer that has finished.
     * @param transfers - the map with active transfers this transfer belongs to (e.g. writes or reads).
     * @param transferType - the type of this transfer, e.g. "write".
     * @return true if this was the last transfer in the job, false otherwise.
     */
    private boolean onTransferFinished(GlobalStorageTransfer transfer, Map<Job, List<GlobalStorageTransfer>> transfers,
            String transferType) {
        String logMsg = String.format("Global %s transfer %s finished: %s, bytes transferred: %d, duration: %f",
                transferType, transfer.getId(), transfer.getFile().getName(), transfer.getFile().getSize(),
                transfer.getDuration());
        getCloudsim().log(logMsg);
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
        case WorkflowEvent.GLOBAL_STORAGE_READ_PROGRESS:
            onReadProgress((GlobalStorageTransfer) ev.getData());
            break;
        case WorkflowEvent.GLOBAL_STORAGE_WRITE_PROGRESS:
            onWriteProgress((GlobalStorageTransfer) ev.getData());
            break;
        default:
            super.onUnknownSimEvent(ev);
            break;
        }

        logStorageState();
    }

    private int lastNumReads = -1;
    private int lastNumWrites = -1;

    private void logStorageState() {
        if (hasStorageStateNotChanged()) {
            return;
        }
        getCloudsim().log(
                String.format("GS state has changed: readers = %d, writers = %d, read_speed = %f, write_speed = %f",
                        congestedParams.getNumReads(), congestedParams.getNumWrites(), congestedParams.getReadSpeed(),
                        congestedParams.getWriteSpeed()));

        lastNumReads = congestedParams.getNumReads();
        lastNumWrites = congestedParams.getNumWrites();
    }

    private boolean hasStorageStateNotChanged() {
        return lastNumReads == congestedParams.getNumReads() && lastNumWrites == congestedParams.getNumWrites();
    }

    /** Called on GLOBAL_STORAGE_WRITE_PROGRESS event. */
    private void onWriteProgress(GlobalStorageTransfer write) {
        if (write.isCompleted()) {
            getCloudsim().sendNow(getId(), getId(), WorkflowEvent.GLOBAL_STORAGE_WRITE_FINISHED, write);
        } else {
            progressTransfer(write, WorkflowEvent.GLOBAL_STORAGE_WRITE_PROGRESS, congestedParams.getWriteSpeed());
        }
    }

    /** Called on GLOBAL_STORAGE_READ_FINISHED event */
    private void onReadProgress(GlobalStorageTransfer read) {
        if (read.isCompleted()) {
            getCloudsim().sendNow(getId(), getId(), WorkflowEvent.GLOBAL_STORAGE_READ_FINISHED, read);
        } else {
            progressTransfer(read, WorkflowEvent.GLOBAL_STORAGE_READ_PROGRESS, congestedParams.getReadSpeed());
        }
    }

    /**
     * Progresses transfer by transferring some amount of bytes for params.getChunkTransferTime() time. If there are
     * less bytes to transfer than we can we transfer for shorter time.
     * 
     * @param transfer the transfer to progress
     * @param progressEvent event sent after this progress
     * @param speed transfer speed
     */
    private void progressTransfer(GlobalStorageTransfer transfer, int progressEvent, double speed) {
        double bytesTransferred = speed * params.getChunkTransferTime();
        double time = 0.0;
        // There are less bytes to transfer that we want
        if (bytesTransferred > transfer.getRemainingBytesToTransfer()) {
            bytesTransferred = transfer.getRemainingBytesToTransfer();
            time = bytesTransferred / speed;
        } else {
            time = params.getChunkTransferTime();
        }
        transfer.addDuration(time);
        transfer.addBytesTransferred(bytesTransferred);
        getCloudsim().sendToMyself(this, time, progressEvent, transfer);
    }

    /**
     * Trivial transfer estimation based o read and write speeds. This seems good enough, but we might change the
     * implementation in the future
     * 
     * @see StorageManager#getTransferTimeEstimation(Task)
     */
    @Override
    public double getTransferTimeEstimation(Task task) {
        double time = 0.0;
        for (DAGFile file : task.getInputFiles()) {
            time += file.getSize() / params.getReadSpeed();
            time += params.getLatency();
        }
        for (DAGFile file : task.getOutputFiles()) {
            time += file.getSize() / params.getWriteSpeed();
            time += params.getLatency();
        }
        return time;
    }

    /**
     * Simulates congestion.
     * Updates read and write speeds based on numbers of currently active transfer.
     */
    private void updateSpeedCongestion() {
        double writeSpeed = params.getWriteSpeed();
        if (congestedParams.getNumWrites() > 0) {
            writeSpeed = ((double) params.getNumReplicas() * params.getWriteSpeed()) / congestedParams.getNumWrites();
            if (writeSpeed > params.getWriteSpeed()) {
                writeSpeed = params.getWriteSpeed();
            }
        }
        congestedParams.setWriteSpeed(writeSpeed);

        double readSpeed = params.getReadSpeed();
        if (congestedParams.getNumReads() > 0) {
            readSpeed = ((double) params.getNumReplicas() * params.getReadSpeed()) / congestedParams.getNumReads();
            if (readSpeed > params.getReadSpeed()) {
                readSpeed = params.getReadSpeed();
            }
        }
        congestedParams.setReadSpeed(readSpeed);
    }

    public GlobalStorageParams getParams() {
        return params;
    }

    public VMCacheManager getCacheManager() {
        return cacheManager;
    }
}
