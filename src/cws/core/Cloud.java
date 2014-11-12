package cws.core;

import java.util.HashSet;
import java.util.Set;

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
    }

    public void addVMListener(VMListener l) {
        vmListeners.add(l);
    }

    public void removeVMListener(VMListener l) {
        vmListeners.remove(l);
    }
    
    public Set<VM> getAllVms() {
        return vms;
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
        getCloudsim().sendNow(this.getId(), vm.getId(), WorkflowEvent.VM_LAUNCH, null);

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
        getCloudsim().sendNow(this.getId(), vm.getOwner(), WorkflowEvent.VM_LAUNCHED, vm);
    }

    private void terminateVM(VM vm) {
        // Sanity check
        if (!vms.contains(vm)) {
            throw new RuntimeException("Unknown VM: " + vm.getId());
        }
        vm.setTerminated(true);
        // We terminate the VM now...
        getCloudsim().sendNow(this.getId(), vm.getId(), WorkflowEvent.VM_TERMINATE, null);

        // But it isn't gone until after the delay
        getCloudsim().send(getId(), getId(), vm.getDeprovisioningDelay(), WorkflowEvent.VM_TERMINATED, vm);
        vms.remove(vm);
    }

    private void vmTerminated(VM vm) {
        getCloudsim().log(String.format("VM %d terminated", vm.getId()));

        // Listeners find out
        for (VMListener l : vmListeners) {
            l.vmTerminated(vm);
        }
        
        vm.setTerminateTime(getCloudsim().clock());

        // The owner finds out
        getCloudsim().sendNow(this.getId(), vm.getOwner(), WorkflowEvent.VM_TERMINATED, vm);
    }
}
