package cws.core.storage;

import org.cloudbus.cloudsim.core.SimEntity;
import org.cloudbus.cloudsim.core.SimEvent;

import cws.core.WorkflowEvent;
import cws.core.exception.UnknownWorkflowEventException;

public class StorageManager extends SimEntity implements WorkflowEvent {
    public StorageManager() {
        super("StorageManager");
    }

    @Override
    public void processEvent(SimEvent ev) {
        System.err.println("RECEIVED");
        switch (ev.getTag()) {
        case NEW_FILE_TRANSFER:
            break;
        case UPDATE_FILE_TRANSFER_PROGRESS:
            break;
        case FILE_TRANSFER_COMPLETE:
            break;
        default:
            throw new UnknownWorkflowEventException("Unknown event: " + ev);
        }
    }

    @Override
    public void startEntity() {
        // do nothing
    }

    @Override
    public void shutdownEntity() {
        // do nothing
    }
}
