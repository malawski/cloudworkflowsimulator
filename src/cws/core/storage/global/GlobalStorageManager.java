package cws.core.storage.global;

import cws.core.Job;
import cws.core.storage.FileTransfer;
import cws.core.storage.StorageManager;

/**
 * TODO(bryk): comment
 */
public class GlobalStorageManager extends StorageManager {
    /**
     * Average read speed of the storage.
     * TODO(bryk): randomize under some distribution r/w speeds.
     */
    private double readSpeed;

    /**
     * Average write speed of the storage.
     * TODO(bryk): randomize under some distribution r/w speeds.
     */
    private double writeSpeed;

    /**
     * Initializes GlobalStorageManager with the appropriate parameters. Check their documentation for more information.
     */
    public GlobalStorageManager(double readSpeed, double writeSpeed) {
        this.readSpeed = readSpeed;
        this.writeSpeed = writeSpeed;
    }

    /**
     * When the file write has completed it's marked as available for reading.
     * @param write - the write which has completed
     */
    private void onWriteCompleted(GlobalStorageWrite write) {
        // TODO(bryk): implement it
    }

    /**
     * When the file read has completed we should:
     * * inform job's VM if all imput files are available
     * * read another input file
     * @param read
     */
    private void onReadCompleted(GlobalStorageRead read) {
        // TODO(bryk): implement it
    }

    /**
     * Reads job's input files from the global storage.
     */
    @Override
    public void onBeforeJobStart(Job job) {
        // TODO(bryk): implement it
    }

    /**
     * Transfers out job's output files to the global storage.
     */
    @Override
    public void onAfterJobFinish(Job job) {
        // TODO(bryk): implement it
    }

    /**
     * When the transfer completes this methods checks if it was write or read and then delegates execution to either
     * {@link #onWriteCompleted(GlobalStorageWrite)} or {@link #onReadCompleted(GlobalStorageRead)}
     */
    @Override
    public void onFileTransferCompleted(FileTransfer fileTransfer) {
        if (fileTransfer instanceof GlobalStorageRead) {
            onReadCompleted((GlobalStorageRead) fileTransfer);
        } else if (fileTransfer instanceof GlobalStorageWrite) {
            onWriteCompleted((GlobalStorageWrite) fileTransfer);
        }
    }
}
