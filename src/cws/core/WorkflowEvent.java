package cws.core;

public interface WorkflowEvent {
    // ///////////////////////////////////////////////////////
    // TRANSFER EVENTS
    // ///////////////////////////////////////////////////////

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

    // ///////////////////////////////////////////////////////
    // VM EVENTS
    // ///////////////////////////////////////////////////////

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

    /** Client entity submits a VM termination request */
    public static final int TERMINATE_VM = 10;

    /** VM termination is complete */
    public static final int VM_TERMINATION_COMPLETE = 11;

    /** Start a new VM */
    public static final int VM_LAUNCH = 12;

    /** VM started up */
    public static final int VM_LAUNCHED = 13;

    /** Terminate an existing VM */
    public static final int VM_TERMINATE = 14;

    /** VM was terminated */
    public static final int VM_TERMINATED = 15;

    // ///////////////////////////////////////////////////////
    // JOB EVENTS
    // ///////////////////////////////////////////////////////

    /** Submit a new task */
    public static final int JOB_SUBMIT = 16;

    /** Job begins execution on remote resource */
    public static final int JOB_STARTED = 17;

    /** Job finished execution on remote host */
    public static final int JOB_FINISHED = 18;

    // ///////////////////////////////////////////////////////
    // DAG EVENTS
    // ///////////////////////////////////////////////////////

    public static final int DAG_SUBMIT = 19;

    public static final int DAG_STARTED = 20;

    public static final int DAG_FINISHED = 21;

    // ///////////////////////////////////////////////////////
    // PROVISIONING EVENTS
    // ///////////////////////////////////////////////////////

    /** Submit next provisioning request */
    public static final int PROVISIONING_REQUEST = 22;

    // ///////////////////////////////////////////////////////
    // STORAGE EVENTS
    // ///////////////////////////////////////////////////////

    /** Sent just before the start of a task. Input files should be then transferred */
    int STORAGE_BEFORE_TASK_START = 29;

    /** Sent just after the finish of a task. Output files should be then transferred */
    int STORAGE_AFTER_TASK_COMPLETED = 30;

    /** Sent after all input files have been transferred to a task */
    int STORAGE_ALL_BEFORE_TRANSFERS_COMPLETED = 32;

    /** Sent after all output files have been transferred out from a task */
    int STORAGE_ALL_AFTER_TRANSFERS_COMPLETED = 33;
}
