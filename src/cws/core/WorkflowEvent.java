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
    
    /////////////////////////////////////////////////////////
    // VM EVENTS
    /////////////////////////////////////////////////////////
    
    /** Client entity submits a new VM creation request */
    public static final int NEW_VM = 5;
    
    /** VM creation is complete */
    public static final int VM_CREATION_COMPLETE = 6;
    
    /** Client entity submits a new cloudlet to a VM */
    public static final int CLOUDLET_SUBMIT = 7;

    /** Cloudlet execution is started */
    public static final int CLOUDLET_STARTED = 8;

    /** Cloudlet execution is comlplete */
    public static final int CLOUDLET_COMPLETE = 9;

    
}
