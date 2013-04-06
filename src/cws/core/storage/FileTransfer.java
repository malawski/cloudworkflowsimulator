package cws.core.storage;

/**
 * TODO(bryk) : comment
 */
public abstract class FileTransfer {
    /**
     * Transferring file's name
     */
    private String fileName;
    /**
     * After the transfer has finished this listener will be informed about this. <br>
     * Event name: FILE_TRANSFER_COMPLETED
     */
    private int listenerId;

    public FileTransfer(String fileName, int listenerId) {
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
