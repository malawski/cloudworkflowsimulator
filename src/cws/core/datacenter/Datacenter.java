package cws.core.datacenter;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.SimEntity;
import org.cloudbus.cloudsim.core.SimEvent;

import cws.core.WorkflowEvent;


/** 
 * This is a simple "cloud" which allows creating VMs and submitting cloudlets to these VMs.
 * It does not perform any scheduling, only introduces some delays in the process.
 * 
 * @author malawski
 *
 */
public class Datacenter extends SimEntity implements WorkflowEvent { 

	
    /** A time it takes to create a VM (in seconds) */
    public static final double VM_CREATION_DELAY = 60.0;
    public static final double VM_TERMINATION_DELAY = 60.0;
    public static final double CLOUDLET_SUBMISSION_DELAY = 1.0;
    
    // here we store VM creation and termination times
    private Map<Integer, Double> vmCreationTimes = new HashMap<Integer, Double>();
	private Map<Integer, Double> vmTerminationTimes = new HashMap<Integer, Double>();

    
	/** The set of running VMs */
	private Set<Vm> runningVMs;

	/** The set of pending VMs */
	private Set<Vm> pendingVMs;

	/** The set of free VMs, i.e. the ones which are not executing any cloudlets (idle) */
	private Set<Vm> freeVMs;
	
	/** The set of busy VMs, i.e. the ones which execute cloudlets */
	private Set<Vm> busyVMs;

	/** The set of VMs that are shutting down */
	private Set<Vm> shuttingVMs;
	
	/** The set of terminated VMs */
	private Set<Vm> terminatedVMs;
	
	private Map<Integer,Vm> vmids;

	
	public Datacenter(String name) {
		super(name);
        CloudSim.addEntity(this);
        
        pendingVMs = new HashSet<Vm>();
        runningVMs = new HashSet<Vm>();
        freeVMs = new HashSet<Vm>();
        busyVMs = new HashSet<Vm>();
        shuttingVMs = new HashSet<Vm>();
        terminatedVMs = new HashSet<Vm>();
        
        vmids = new HashMap<Integer, Vm>();
	}

    public Map<Integer, Double> getVmCreationTimes() {
		return vmCreationTimes;
	}

	public Map<Integer, Double> getVmTerminationTimes() {
		return vmTerminationTimes;
	}
	
	
	@Override
	public void startEntity() {
        /* Do nothing */
		
	}

	@Override
	public void processEvent(SimEvent ev) {
        switch(ev.getTag()) {
	        case NEW_VM:
	            newVM((Vm)ev.getData());
	            break;
	        case VM_CREATION_COMPLETE:
	            vmCreationComplete((Vm)ev.getData());
	            break;
	        case CLOUDLET_SUBMIT:
	        	submitCloudlet((Cloudlet)ev.getData());
	        	break;
	        case CLOUDLET_STARTED:
	        	startCloudlet((Cloudlet)ev.getData());
	        	break;
	        case CLOUDLET_COMPLETE:
	        	completeCloudlet((Cloudlet)ev.getData());
	        	break;
	        case TERMINATE_VM:
	        	terminateVM((Vm)ev.getData());
	        	break;
	        case VM_TERMINATION_COMPLETE:
	        	vmTerminationComplete((Vm)ev.getData());
	        	break;
	        default:
	            throw new RuntimeException("Unknown event: "+ev);
	    }
		
	}



	/** Called when a new VM creation request is submitted **/
	private void newVM(Vm vm) {
        // Sanity check
        if (pendingVMs.contains(vm)) {
            throw new RuntimeException("Duplicate vm creation request: "+vm.getId());
        }
		pendingVMs.add(vm);
		vmids.put(vm.getId(), vm);
        // Now we assume that the delay is constant 
        double delay = VM_CREATION_DELAY;
        send(this.getId(), delay, VM_CREATION_COMPLETE, vm);
        Log.printLine(CloudSim.clock() + " Creating VM " + vm.getId());
        vmCreationTimes.put(vm.getId(), CloudSim.clock());
	}
	
	/** Called when a new VM creation request is completed **/
	private void vmCreationComplete(Vm vm) {
        // Sanity check
        if (runningVMs.contains(vm)) {
            throw new RuntimeException("VM already running: "+vm.getId());
        }
		pendingVMs.remove(vm);
		runningVMs.add(vm);
		freeVMs.add(vm);
		sendNow(vm.getUserId(), VM_CREATION_COMPLETE, vm);
        Log.printLine(CloudSim.clock() + " Created VM " + vm.getId());		
	}
	
	/** Called when a VM termination request is received */
	private void terminateVM(Vm vm) {
        // Sanity check - we assume now that only free running VMs can be terminated
        if (!runningVMs.contains(vm)) {
            throw new RuntimeException("Trying to terminate a VM that is not running: "+vm.getId());
        }
        if (busyVMs.contains(vm)) {
            throw new RuntimeException("Trying to terminate a VM that is busy: "+vm.getId());
        }
        if (!freeVMs.contains(vm)) {
            throw new RuntimeException("Trying to terminate a VM that is not free: "+vm.getId());
        }
        freeVMs.remove(vm);
        shuttingVMs.add(vm);
     // Now we assume that the delay is constant 
        double delay = VM_TERMINATION_DELAY;
        send(this.getId(), delay, VM_TERMINATION_COMPLETE, vm);
        Log.printLine(CloudSim.clock() + " Shutting down VM " + vm.getId());
	}


	private void vmTerminationComplete(Vm vm) {
        // Sanity check - we assume now that only free running VMs can be terminated
        if (!shuttingVMs.contains(vm)) {
            throw new RuntimeException("The VM should be in shutting down state: "+vm);
        }
        shuttingVMs.remove(vm);
        terminatedVMs.add(vm);
        vmTerminationTimes.put(vm.getId(), CloudSim.clock());
        sendNow(vm.getUserId(), VM_TERMINATION_COMPLETE, vm);		
	}
	
	/** Called when a cloudlet submission request is submitted **/
	private void submitCloudlet(Cloudlet cloudlet) {

		Vm vm = vmids.get(cloudlet.getVmId());

        // Sanity check
        if (busyVMs.contains(vm)) {
            throw new RuntimeException("VM already busy: "+vm);
        }
        freeVMs.remove(vm);
        busyVMs.add(vm);
        // Now we assume that the delay is constant 
        double delay = CLOUDLET_SUBMISSION_DELAY;
        send(this.getId(), delay, CLOUDLET_STARTED, cloudlet);
        cloudlet.setSubmissionTime(CloudSim.clock());		
	}
	
	/** Called when a cloudlet execution starts **/
	private void startCloudlet(Cloudlet cloudlet) {
		double executionTime = computeExecutionTime(cloudlet);
		send(this.getId(), executionTime, CLOUDLET_COMPLETE, cloudlet);
		sendNow(cloudlet.getUserId(), CLOUDLET_STARTED, cloudlet);
		cloudlet.setExecStartTime(CloudSim.clock());	
	}
	
	/** 
	 * Compute the execution time of a cloudlet on a VM, 
	 * based on cloudlet length (in million instructions)
	 * and on MIPS assigned to a VM
	 */
	private double computeExecutionTime(Cloudlet cloudlet) {
		Vm vm = vmids.get(cloudlet.getVmId());
		double MIPS = vm.getMips();
		double MI=cloudlet.getCloudletLength();
		double executionTime = MI/MIPS;
		return executionTime;
	}
	
	/** Called when a cloudlet execution is complete **/
	private void completeCloudlet(Cloudlet cloudlet) {
		Vm vm = vmids.get(cloudlet.getVmId());
        // Sanity check
        if (freeVMs.contains(vm)) {
            throw new RuntimeException("VM should be busy busy: "+vm);
        }
        busyVMs.remove(vm);
        freeVMs.add(vm);
        double wallTime = CloudSim.clock()-cloudlet.getSubmissionTime();
        double actualTime = CloudSim.clock()-cloudlet.getExecStartTime();
        cloudlet.setExecParam(wallTime, actualTime);
        try {
			cloudlet.setCloudletStatus(Cloudlet.SUCCESS);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
        sendNow(cloudlet.getUserId(), CLOUDLET_COMPLETE, cloudlet);		
	}
	
	
	@Override
	public void shutdownEntity() {
        /* Do nothing */
		
	}

}
