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
    /** Actual bytes read (may be lower than totalFilesToRead beacause of cache) */
    private long actualBytesRead;
    /** Total number of files requested to read */
    private int totalFilesToRead;
    /** Total number of files requested to write */
    private int totalFilesToWrite;
    /** Actual number of files read (may be lower than totalFilesToRead beacause of cache) */
    private int actualFilesRead;

    public long getTotalBytesToRead() {
        return totalBytesToRead;
    }

    public void addBytesToRead(long num) {
        this.totalBytesToRead += num;
    }

    public long getTotalBytesToWrite() {
        return totalBytesToWrite;
    }

    public void addBytesToWrite(long num) {
        this.totalBytesToWrite += num;
    }

    public long getActualBytesRead() {
        return actualBytesRead;
    }

    public void addActualBytesRead(long num) {
        this.actualBytesRead += num;
    }

    public int getTotalFilesToRead() {
        return totalFilesToRead;
    }

    public void addTotalFilesToRead(int totalFilesToRead) {
        this.totalFilesToRead += totalFilesToRead;
    }

    public int getTotalFilesToWrite() {
        return totalFilesToWrite;
    }

    public void addTotalFilesToWrite(int totalFilesToWrite) {
        this.totalFilesToWrite += totalFilesToWrite;
    }

    public int getActualFilesRead() {
        return actualFilesRead;
    }

    public void addActualFilesRead(int actualFilesRead) {
        this.actualFilesRead += actualFilesRead;
    }
}
