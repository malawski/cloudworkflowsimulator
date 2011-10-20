package cws.core;

public interface WorkflowEvent {
    /////////////////////////////////////////////////////////
    // TRANSFER EVENTS
    /////////////////////////////////////////////////////////
    
    /** Client entity submits a new transfer */
    public static final int NEW_TRANSFER = 0;
    
    /** Update the progress of existing transfers */
    public static final int UPDATE_TRANSFER_PROGRESS = 1;
    
    /** Transfer's initial handshake is complete */
    public static final int HANDSHAKE_COMPLETE = 2;
    
    /** Transfer's final ACK received */
    public static final int FINAL_ACK_RECEIVED = 3;
    
    /** Transfer is complete */
    public static final int TRANSFER_COMPLETE = 4;
}
