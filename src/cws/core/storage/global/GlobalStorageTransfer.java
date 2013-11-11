package cws.core.storage.global;

import cws.core.dag.DAGFile;
import cws.core.jobs.Job;

/**
 * Describes global storage transfer. This can be either read or write.
 */
public class GlobalStorageTransfer {
    /** The job this transfer transfers file from/to */
    private Job job;
    /** Transferred file */
    private DAGFile file;
    /** Number of bytes transferred so far */
    private double bytesTransferred = 0;
    /** Unique transfer task identifier */
    private int id;

    /** Next ID for a transfer task */
    private static int next_id = 0;

    /**
     * Transfer's duration. It should have proper value after transfer finish. In the meantime it can have some
     * intermediate increasing value.
     */
    private double duration;

    /**
     * @param job - the job this transfer transfers file from/to
     * @param file - the transferred file
     */
    public GlobalStorageTransfer(Job job, DAGFile file) {
        this.id = next_id++;

        this.job = job;
        this.file = file;
    }

    /**
     * @return is the transfer completed? I.e. all bytes are transferred?
     */
    public boolean isCompleted() {
        return bytesTransferred >= file.getSize();
    }

    public double getRemainingBytesToTransfer() {
        double bytes = file.getSize() - bytesTransferred;
        if (bytes < 0)
            return 0;
        else
            return bytes;
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof GlobalStorageTransfer && job.equals(((GlobalStorageTransfer) obj).job);
    }

    @Override
    public int hashCode() {
        return job.hashCode();
    }

    public Job getJob() {
        return job;
    }

    public DAGFile getFile() {
        return file;
    }

    public double getDuration() {
        return duration;
    }

    public void addBytesTransferred(double amountBytes) {
        bytesTransferred += amountBytes;
    }

    public void addDuration(double amount) {
        duration += amount;
    }

    public int getId() {
        return id;
    }
}
