package cws.core.storage.global;

import cws.core.dag.DAGFile;
import cws.core.jobs.Job;

/**
 * TODO(bryk): comment
 */
public class GlobalStorageTransfer {
    /** The job this transfer transfers file from/to */
    private Job job;
    /** Transferred file */
    private DAGFile file;

    /**
     * Transfer's duration. It should have proper value after transfer finish. In the meantime it can have some
     * intermediate
     * increasing value
     */
    private double duration;

    /**
     * @param job - the job this transfer transfers file from/to
     * @param name - transferred file's name
     * @param size - size of the transferred file TODO(bryk): determine unit
     */
    public GlobalStorageTransfer(Job job, DAGFile file) {
        this.job = job;
        this.file = file;
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

    public void setDuration(double duration) {
        this.duration = duration;
    }
}
