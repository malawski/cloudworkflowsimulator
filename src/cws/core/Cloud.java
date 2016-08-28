package cws.core;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;

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

    /** The set of VMs which are available for use (this is different to
     * _vmsForSanityCheck because it does not include VMs which are waiting
     * to be launched). This is the collection that was previously returned
     * by getAllVMs().
     */
    private final Set<VM> availableVMs = new HashSet<VM>();

    /** The set of currently active VMs, used for internal sanity checks
     * only. */
    private final Set<VM> _vmsForSanityCheck = new HashSet<VM>();


    private final Set<VMListener> vmListeners = new HashSet<VMListener>();

    public Cloud(CloudSimWrapper cloudsim) {
        super("Cloud", cloudsim);
    }

    public void addVMListener(VMListener l) {
        vmListeners.add(l);
    }

    public List<VM> getAvailableVMs() {
        return ImmutableList.copyOf(availableVMs);
    }

    public List<VM> getFreeVMs() {
        Builder<VM> free = ImmutableList.<VM>builder();
        for (VM vm : availableVMs) {
            if (!vm.isTerminated() && vm.isFree()) {
                free.add(vm);
            }
        }
        return free.build();
    }

    public List<VM> getBusyVMs() {
        Builder<VM> busy = ImmutableList.<VM>builder();
        for (VM vm : availableVMs) {
            if (!vm.isTerminated() && !vm.isIdle()) {
                busy.add(vm);
            }
        }
        return busy.build();
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

    /**
     * Launches the given VM and adds it to acvite VMs pool.
     */
    public void launchVM(int owner, VM vm) {
        vm.setOwner(owner);
        vm.setCloud(getId());
        vm.setLaunchTime(getCloudsim().clock());
        _vmsForSanityCheck.add(vm);

        // We launch the VM now...
        vm.launch();

        // But it isn't ready until after the delay
        getCloudsim().send(getId(), getId(), vm.getProvisioningDelay(), WorkflowEvent.VM_LAUNCHED, vm);
    }

    /** Launches the given VM at some time in the future.
     */
    public void launchVMAtTime(int id, VM vm, double launchTime) {
        final double now = getCloudsim().clock();
        if(launchTime < now) {
            throw new IllegalArgumentException("Cannot launch a VM in the past.");
        }

        // Send message to ourself to launch the VM when it is needed.
        final double delay = launchTime - now;
        getCloudsim().send(id, getId(), delay, WorkflowEvent.VM_LAUNCH, vm);
    }

    private void vmLaunched(VM vm) {
        // Sanity check
        if (!_vmsForSanityCheck.contains(vm)) {
            throw new RuntimeException("Unknown VM");
        }

        // VM is now available
        availableVMs.add(vm);

        // Listeners are informed
        for (VMListener l : vmListeners) {
            l.vmLaunched(vm);
        }

        // The owner learns about the launch
        getCloudsim().sendNow(this.getId(), vm.getOwner(), WorkflowEvent.VM_LAUNCHED, vm);
    }

    /**
     * Terminates the given VM. It will still be charged for the deprovisioning delay.
     */
    public final void terminateVM(VM vm) {
        // Sanity check
        if (!_vmsForSanityCheck.contains(vm)) {
            throw new RuntimeException("Unknown VM: " + vm.getId());
        }
        // We terminate the VM now...
        vm.terminate();

        // But it isn't gone until after the delay
        getCloudsim().send(getId(), getId(), vm.getDeprovisioningDelay(), WorkflowEvent.VM_TERMINATED, vm);
        _vmsForSanityCheck.remove(vm);
    }

    private void vmTerminated(VM vm) {
        getCloudsim().log(String.format("VM %d terminated", vm.getId()));

        // VM is no longer available
        availableVMs.remove(vm);

        // Listeners find out
        for (VMListener l : vmListeners) {
            l.vmTerminated(vm);
        }

        vm.setTerminateTime(getCloudsim().clock());

        // The owner finds out
        getCloudsim().sendNow(this.getId(), vm.getOwner(), WorkflowEvent.VM_TERMINATED, vm);
    }
}
