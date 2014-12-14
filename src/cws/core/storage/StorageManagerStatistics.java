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
    /** The number of bytes read from cache. */
    private long bytesReadFromCache;
    /** Total number of files requested to read */
    private int totalFilesToRead;
    /** Total number of files requested to write */
    private int totalFilesToWrite;
    /** The number of files read from cache. */
    private int filesReadFromCache;

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

    public long getBytesReadFromCache() {
        return bytesReadFromCache;
    }

    public void addBytesReadFromCache(long num) {
        this.bytesReadFromCache += num;
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

    public int getFilesReadFromCache() {
        return filesReadFromCache;
    }

    public void addFilesReadFromCache(int filesRead) {
        this.filesReadFromCache += filesRead;
    }
}
