package cws.core.storage.global;

/**
 * TODO(bryk): we could read those parameters from a .properties file.
 * Class containing all parameters for {@link GlobalStorageManager}
 */
public class GlobalStorageParams {
    /** Average read speed of the storage */
    private double readSpeed;

    /** Average write speed of the storage */
    private double writeSpeed;

    /** Average latency for each operation: */
    private double latency;

    /**
     * Amount of time spent on transferring one chunk of a file. Should be relatively small, but not too small because
     * we might face some significant floating point arithmetic errors.
     */
    private double chunkTransferTime = 1;

    public double getReadSpeed() {
        return readSpeed;
    }

    public void setReadSpeed(double readSpeed) {
        this.readSpeed = readSpeed;
    }

    public double getWriteSpeed() {
        return writeSpeed;
    }

    public void setWriteSpeed(double writeSpeed) {
        this.writeSpeed = writeSpeed;
    }

    public double getLatency() {
        return latency;
    }

    public void setLatency(double latency) {
        this.latency = latency;
    }

    public double getChunkTransferTime() {
        return chunkTransferTime;
    }

    public void setChunkTransferTime(double chunkTransferTime) {
        this.chunkTransferTime = chunkTransferTime;
    }
}
