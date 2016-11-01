package cws.core.scheduler;

import cws.core.Scheduler;
import cws.core.VM;
import cws.core.VMFactory;
import cws.core.WorkflowEngine;
import cws.core.cloudsim.CWSSimEntity;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.core.VMType;
import cws.core.engine.Environment;

/**
 * This scheduler submits jobs to VMs on FCFS basis.
 * Job is submitted to VM only if VM is idle (no queuing in VMs).
 * @author malawski
 */
abstract class DAGDynamicScheduler extends CWSSimEntity implements Scheduler {
    protected final Environment environment;
    protected final VMType vmType;

    public DAGDynamicScheduler(CloudSimWrapper cloudsim, Environment environment, VMType vmType) {
        super("DAGDynamicScheduler", cloudsim);
        this.environment = environment;
        this.vmType = vmType;
    }

    @Override
    public abstract void scheduleJobs(WorkflowEngine engine);

    protected VM selectBestVM() {
        return VMFactory.createVM(vmType, getCloudsim());
    }
}
