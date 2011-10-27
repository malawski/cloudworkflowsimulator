package cws.core.datacenter;

import static org.junit.Assert.assertEquals;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.SimEntity;
import org.cloudbus.cloudsim.core.SimEvent;
import org.junit.Test;

import cws.core.Cloud;
import cws.core.Job;
import cws.core.VM;
import cws.core.WorkflowEvent;
import cws.core.dag.DAG;



public class CloudTest {
	
	
	
	/**
	 * This cloud client schedules DAGs by submitting only eligible jobs 
	 * (the ones which have dependencies satisfied) to VMs.
	 * 
	 * @author malawski
	 *
	 */
	private class CloudDAGClient extends CloudClient {

		Set<Job> eligibleJobs;
		
		protected DAG dag;
		
		public CloudDAGClient(String name) {
			super(name);
			eligibleJobs = new HashSet<Job>();
		}

		public void setDAG(DAG dag) {
			this.dag = dag;
		}
		
		
		
		protected void scheduleJobs() {
			
			// if there is nothing to do, just return
			if (freeVMs.isEmpty()) return;
			if (newJobs.isEmpty()) return;
			Iterator<Job> jobIt = eligibleJobs.iterator();
			Iterator<VM> vmIt = freeVMs.iterator();
			while (jobIt.hasNext() && vmIt.hasNext()) {
				Job job = jobIt.next();
				VM vm = vmIt.next();
				job.setVM(vm);
				vmIt.remove(); // remove VM from free set
				busyVMs.add(vm);
				jobIt.remove(); // remove job from eligible set
				submittedJobs.add(job);
				sendNow(vm.getId(), JOB_SUBMIT, job);				
			}		
		}
		
//      @TODO		
//		protected void completeCloudlet(Cloudlet cloudlet) {
//	    	Vm vm = vmids.get(cloudlet.getVmId());
//	    	busyVMs.remove(vm);
//	    	freeVMs.add(vm);
//	    	runningCloudlets.remove(cloudlet);
//	    	completedCloudlets.add(cloudlet);
//			dag.processCloudletReturn(cloudlet);
//	    	scheduleCloudlets();
//		}
//		
		
	}
	
	
	/**
	 * The client class which acts as workflow engine. This one does not handle any dependencies, 
	 * just submits jobs to free VMs.
	 * 
	 * @author malawski
	 *
	 */
	private class CloudClient extends SimEntity implements WorkflowEvent {

		protected Cloud cloud;
		/** The set of VMs. */
		private Set<VM> vms;
		
		/** The set of submitted VMs (not running yet) */
		protected Set<VM> submittedVMs;
		
		/** The set of VMs that are running */
		protected Set<VM> runningVMs;
		
		/** The set of free VMs, i.e. the ones which are not executing any jobs (idle) */
		protected Set<VM> freeVMs;
		
		/** The set of busy VMs, i.e. the ones which execute jobs */
		protected Set<VM> busyVMs;
		
		/** The set of VMs that are shutting down */
		protected Set<VM> shuttingVMs;
		
		/** The set of terminated VMs */
		protected Set<VM> terminatedVMs;

		/** The set of new (unsubmitted) cloudlets */
		protected Set<Job> newJobs;
		
		/** The set of submitted cloudlets */
		protected Set<Job> submittedJobs;
		
		/** The set of running cloudlets */
		protected Set<Job> runningJobs;
		
		/** The set of completed cloudlets */
		protected Set<Job> completedJobs;
		

		protected Set<Job> jobs;
		
		/** map from ids to VM objects - probably should be moved to a global scope */
//		protected Map<Integer,VM> vmids;
		
		public CloudClient(String name) {
			super(name);
			CloudSim.addEntity(this);
			jobs = new HashSet<Job>();
			runningVMs = new HashSet<VM>();
	        freeVMs = new HashSet<VM>();
	        busyVMs = new HashSet<VM>();
	        submittedVMs = new HashSet<VM>();
	        shuttingVMs = new HashSet<VM>();
	        terminatedVMs = new HashSet<VM>();
	        newJobs = new HashSet<Job>();
	        submittedJobs = new HashSet<Job>();
	        runningJobs = new HashSet<Job>();
	        completedJobs = new HashSet<Job>();
		}
		
		public Set<Job> getCompletedJobs() {
			return completedJobs;
		}
		
		public Set<Job> getJobs() {
			return jobs;
		}

		public void setJobs(Set<Job> jobs) {
			this.jobs = jobs;
		}

		public void setVMs(Set<VM> vms) {
			this.vms = vms;
		}

		public void setCloud(Cloud cloud) {
			this.cloud = cloud;
		}
		
		public Set<VM> getRunningVMs() {
			return runningVMs;
		}
		
		public Set<VM> getTerminatedVMs() {
			return terminatedVMs;
		}

		@Override
		public void startEntity() {
			submitVMs(vms);
			newJobs.addAll(jobs);
			scheduleJobs();
		}
		
		
		protected void submitVMs (Set<VM> vms) {
			for (VM vm : vms) {
				send(cloud.getId(), 0.0, VM_LAUNCH, vm);
				submittedVMs.add(vm);
			}
		}

		protected void scheduleJobs() {
			
			// if there is nothing to do, just return
			if (freeVMs.isEmpty()) return;
			if (newJobs.isEmpty()) return;
			Iterator<Job> jobIt = newJobs.iterator();
			Iterator<VM> vmIt = freeVMs.iterator();
			while (jobIt.hasNext() && vmIt.hasNext()) {
				Job job = jobIt.next();
				VM vm = vmIt.next();
				job.setVM(vm);
				vmIt.remove(); // remove VM from free set
				busyVMs.add(vm);
				jobIt.remove(); // remove job from new set
				submittedJobs.add(job);
				sendNow(vm.getId(), JOB_SUBMIT, job);
			}		
		}	

		@Override
		public void processEvent(SimEvent ev) { 
	        switch(ev.getTag()) {
	        case VM_LAUNCHED:
	        	vmLaunched((VM)ev.getData());
	            break;
	        case JOB_STARTED:
	        	startJob((Job)ev.getData());
	            break;
	        case JOB_FINISHED:
	        	completeJob((Job)ev.getData());
	            break;
	        case VM_TERMINATED:
	        	vmTerminationComplete((VM)ev.getData());
	        	break;
	        default:
	            throw new RuntimeException("Unknown event: "+ev);
	        }
		}
		
		protected void vmLaunched(VM vm) {
			Log.printLine(CloudSim.clock() + " VM started " + vm.getId());
            runningVMs.add(vm);
            freeVMs.add(vm);
            submittedVMs.remove(vm);
            scheduleJobs();
		}

		protected void vmTerminationComplete(VM vm) {
			shuttingVMs.remove(vm);
			runningVMs.remove(vm);
			terminatedVMs.add(vm);
		}

		protected void startJob(Job job) {
			Log.printLine(CloudSim.clock() + " Job started " + job.getID());
			submittedJobs.remove(job);
			runningJobs.add(job);
		}
		
		protected void completeJob(Job job) {
	    	VM vm = job.getVM();
	    	busyVMs.remove(vm);
	    	freeVMs.add(vm);
	    	runningJobs.remove(job);
	    	completedJobs.add(job);
	    	scheduleJobs();
		}

		@Override
		public void shutdownEntity() {	}
		
	}
	
    @Test
    public void testDatacenterVMs() {
        CloudSim.init(1, null, false);
        
        CloudClient cloudClient = new CloudClient("Client");
        Cloud cloud  = new Cloud();
        
        cloudClient.setCloud(cloud);
        
        VM vm = new VM(1000, 1, 1.0, 1.0);
        HashSet<VM> vms = new HashSet<VM>();
        vms.add(vm);
                      
        cloudClient.setVMs(vms);
        
        CloudSim.startSimulation();   
        
        assertEquals(vms.size(), cloudClient.getRunningVMs().size());
       
    }
	
    @Test
    public void testDatacenter10VMs() {
        CloudSim.init(1, null, false);
        
        CloudClient cloudClient = new CloudClient("Client");
        Cloud cloud  = new Cloud();
        
        cloudClient.setCloud(cloud);
        HashSet<VM> vms = new HashSet<VM>();
        for (int i = 0; i < 10; i++ ) {
        	VM vm = new VM(1000, 1, 1.0, 1.0);
        	vms.add(vm);
        }     
        cloudClient.setVMs(vms);
        
        CloudSim.startSimulation();   
        
        assertEquals(vms.size(), cloudClient.getRunningVMs().size());
       
    }
    

	@Test
	public void testDatacenterJobs() {
		CloudSim.init(1, null, false);

		CloudClient cloudClient = new CloudClient("Client");
		Cloud cloud = new Cloud();

		cloudClient.setCloud(cloud);
		HashSet<VM> vms = new HashSet<VM>();
		for (int i = 0; i < 10; i++) {
			VM vm = new VM(1000, 1, 1.0, 1.0);
			vms.add(vm);
		}

		HashSet<Job> jobs = new HashSet<Job>();

		for (int i = 0; i < 100; i++) {
			Job job = new Job(1000);
			jobs.add(job);
		}
		cloudClient.setVMs(vms);
		cloudClient.setJobs(jobs);

		CloudSim.startSimulation();

		assertEquals(vms.size(), cloudClient.getRunningVMs().size());
		assertEquals(jobs.size(), cloudClient.getCompletedJobs().size());

	}

}
