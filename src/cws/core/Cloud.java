package cws.core;

import java.util.HashSet;

import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.SimEntity;
import org.cloudbus.cloudsim.core.SimEvent;

/**
 * A Cloud is an entity that handles the provisioning and deprovisioning
 * of VM resources.
 *
 * @author Gideon Juve <juve@usc.edu>
 */
public class Cloud extends SimEntity implements WorkflowEvent {
    
    /** The set of currently active VMs */
    private HashSet<VM> vms = new HashSet<VM>();
    
    private HashSet<VMListener> vmListeners = new HashSet<VMListener>();
    
    public Cloud() {
        super("Cloud");
        CloudSim.addEntity(this);
    }
    
    public void addVMListener(VMListener l) {
        vmListeners.add(l);
    }
    
    public void removeVMListener(VMListener l) {
        vmListeners.remove(l);
    }
    
    @Override
    public void startEntity() {
        // DO NOTHING
    }

    @Override
    public void processEvent(SimEvent ev) {
        switch(ev.getTag()) {
            case VM_LAUNCH:
                launchVM(ev.getSource(), (VM)ev.getData());
                break;
            case VM_TERMINATE:
                terminateVM((VM)ev.getData());
                break;
            case VM_LAUNCHED:
                vmLaunched((VM)ev.getData());
                break;
            case VM_TERMINATED:
                vmTerminated((VM)ev.getData());
                break;
            default:
                throw new RuntimeException("Unknown event: "+ev);
        }
    }

    @Override
    public void shutdownEntity() {
        // DO NOTHING
    }
    
    private void launchVM(int owner, VM vm) {
        vm.setOwner(owner);
        vm.setCloud(getId());
        vm.setLaunchTime(CloudSim.clock());
        vms.add(vm);
        
        // We launch the VM now...
        sendNow(vm.getId(), VM_LAUNCH);
        
        // But it isn't ready until after the delay
        send(getId(), vm.getProvisioningDelay(), VM_LAUNCHED, vm);
    }
    
    private void vmLaunched(VM vm) {
        // Listeners are informed
        for (VMListener l : vmListeners) {
            l.vmLaunched(vm);
        }
        
        // The owner learns about the launch
        sendNow(vm.getOwner(), VM_LAUNCHED, vm);
    }
    
    private void terminateVM(VM vm) {
        // We terminate the VM now...
        sendNow(vm.getId(), VM_TERMINATE);
        
        // But it isn't gone until after the delay
        send(getId(), vm.getDeprovisioningDelay(), VM_TERMINATED, vm);
    }
    
    private void vmTerminated(VM vm) {
        vm.setTerminateTime(CloudSim.clock());
        vms.remove(vm);
        
        // Listeners find out
        for (VMListener l : vmListeners) {
            l.vmTerminated(vm);
        }
        
        // The owner finds out
        sendNow(vm.getOwner(), VM_TERMINATED, vm);
    }
}
