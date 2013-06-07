package cws.core.storage;

/**
 * Various statistics associated with storage managers.
 * @see {@link StorageManager}
 */
public class StorageManagerStatistics {
    /** Total bytes requested to read */
    private long totalBytesToRead;
    /** Total bytes requested to write */
    private long totalBytesToWrite;
    /** Actual bytes read */
    private long actualBytesRead;
    /** Actual bytes written */
    private long actualBytesWritten;

    public long getTotalBytesToRead() {
        return totalBytesToRead;
    }

    public void setTotalBytesToRead(long totalBytesRead) {
        this.totalBytesToRead = totalBytesRead;
    }

    public void addBytesToRead(long num) {
        this.totalBytesToRead += num;
    }

    public long getTotalBytesToWrite() {
        return totalBytesToWrite;
    }

    public void setTotalBytesToWrite(long totalBytesWritten) {
        this.totalBytesToWrite = totalBytesWritten;
    }

    public void addBytesToWrite(long num) {
        this.totalBytesToWrite += num;
    }

    public long getActualBytesRead() {
        return actualBytesRead;
    }

    public void setActualBytesRead(long actualBytesRead) {
        this.actualBytesRead = actualBytesRead;
    }

    public void addActualBytesRead(long num) {
        this.actualBytesRead += num;
    }

    public long getAcutalBytesWritten() {
        return actualBytesWritten;
    }

    public void setAcutalBytesWritten(long acutalBytesWritten) {
        this.actualBytesWritten = acutalBytesWritten;
    }

    public void addActualBytesWritten(long num) {
        this.actualBytesWritten += num;
    }
}
