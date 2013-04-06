package cws.core.storage.global;

import cws.core.storage.FileTransfer;

/**
 * TODO(bryk): comment
 */
public class GlobalStorageWrite extends FileTransfer {
    private long fileSize;

    public GlobalStorageWrite(String fileName, int listenerId, long fileSize) {
        super(fileName, listenerId);
        this.fileSize = fileSize;
    }

    public long getFileSize() {
        return fileSize;
    }
}
