package cws.core.storage.global;

/**
 * TODO(bryk): comment
 */
public abstract class FileTransferTask /* extends Task */{
    /**
     * Transferring file's name
     */
    private String fileName;
    /**
     * Listener to which send events related to this transfer
     */
    private int listenerId;

    public FileTransferTask(String fileName, int listenerId) {
        this.fileName = fileName;
        this.listenerId = listenerId;
    }

    public String getFileName() {
        return fileName;
    }

    public int getListenerId() {
        return listenerId;
    }
}
