package cws.core.storage.global;

/**
 * TODO(bryk): comment
 */
public class GlobalStorageWrite extends FileTransferTask {
    private long fileSize;

    public GlobalStorageWrite(String fileName, int listenerId, long fileSize) {
        super(fileName, listenerId);
        this.fileSize = fileSize;
    }

    public long getFileSize() {
        return fileSize;
    }
}
