package cws.core;

import java.util.HashSet;

import cws.core.cloudsim.CWSSimEntity;
import cws.core.cloudsim.CWSSimEvent;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.exception.UnknownWorkflowEventException;

/**
 * A Cloud is an entity that handles the provisioning and deprovisioning
 * of VM resources.
 * 
 * @author Gideon Juve <juve@usc.edu>
 */
public class Cloud extends CWSSimEntity {

    /** The set of currently active VMs */
    private HashSet<VM> vms = new HashSet<VM>();

    private HashSet<VMListener> vmListeners = new HashSet<VMListener>();

    public Cloud(CloudSimWrapper cloudsim) {
        super("Cloud", cloudsim);
        cloudsim.addEntity(this);
    }

    public void addVMListener(VMListener l) {
        vmListeners.add(l);
    }

    public void removeVMListener(VMListener l) {
        vmListeners.remove(l);
    }

    @Override
    public void processEvent(CWSSimEvent ev) {
        switch (ev.getTag()) {
        case WorkflowEvent.VM_LAUNCH:
            launchVM(ev.getSource(), (VM) ev.getData());
            break;
        case WorkflowEvent.VM_TERMINATE:
            terminateVM((VM) ev.getData());
            break;
        case WorkflowEvent.VM_LAUNCHED:
            vmLaunched((VM) ev.getData());
            break;
        case WorkflowEvent.VM_TERMINATED:
            vmTerminated((VM) ev.getData());
            break;
        default:
            throw new UnknownWorkflowEventException("Unknown event: " + ev);
        }
    }

    private void launchVM(int owner, VM vm) {
        vm.setOwner(owner);
        vm.setCloud(getId());
        vm.setLaunchTime(getCloudsim().clock());
        vms.add(vm);

        // We launch the VM now...
        sendNow(vm.getId(), WorkflowEvent.VM_LAUNCH);

        // But it isn't ready until after the delay
        getCloudsim().send(getId(), getId(), vm.getProvisioningDelay(), WorkflowEvent.VM_LAUNCHED, vm);
    }

    private void vmLaunched(VM vm) {
        // Sanity check
        if (!vms.contains(vm)) {
            throw new RuntimeException("Unknown VM");
        }

        // Listeners are informed
        for (VMListener l : vmListeners) {
            l.vmLaunched(vm);
        }

        // The owner learns about the launch
        sendNow(vm.getOwner(), WorkflowEvent.VM_LAUNCHED, vm);
    }

    private void terminateVM(VM vm) {
        // Sanity check
        if (!vms.contains(vm)) {
            throw new RuntimeException("Unknown VM");
        }

        // We terminate the VM now...
        sendNow(vm.getId(), WorkflowEvent.VM_TERMINATE);

        // But it isn't gone until after the delay
        getCloudsim().send(getId(), getId(), vm.getDeprovisioningDelay(), WorkflowEvent.VM_TERMINATED, vm);
    }

    private void vmTerminated(VM vm) {
        // Sanity check
        if (!vms.contains(vm)) {
            throw new RuntimeException("Unknown VM");
        }

        vm.setTerminateTime(getCloudsim().clock());
        vms.remove(vm);

        // Listeners find out
        for (VMListener l : vmListeners) {
            l.vmTerminated(vm);
        }

        // The owner finds out
        sendNow(vm.getOwner(), WorkflowEvent.VM_TERMINATED, vm);
    }
}
