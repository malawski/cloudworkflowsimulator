package cws.core.datacenter;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.SimEntity;
import org.cloudbus.cloudsim.core.SimEvent;
import org.junit.Test;

import cws.core.WorkflowEvent;
import cws.core.dag.Job;
import cws.scenarios.CloudletListGenerator;
import cws.scenarios.Helper;
import cws.scenarios.VmListGenerator;


public class DatacenterTest {

	
	
	/**
	 * This datacenter client adds functionality of dynamically provisioning 
	 * and deprovisioning VMs 
	 * 
	 * @author malawski
	 *
	 */
	private class DatacenterDeprovisionerDAGClient extends DatacenterProvisionerDAGClient {
		
		public DatacenterDeprovisionerDAGClient(String name) {
			super(name);
		}
	
		protected void provisionVMs() {
			
			
			// provisioning
			if (job.getEligibleCloudlets().size() > freeVMs.size() + submittedVMs.size()) {
				ArrayList<Vm> vmlist = VmListGenerator.generateVmList(1, getId());
				submitVMs(new HashSet<Vm>(vmlist));
			}
			
			//deprovisioning
			if(job.getEligibleCloudlets().size() < freeVMs.size()) {
				for (Iterator<Vm> it = freeVMs.iterator(); it.hasNext();) {
					Vm vm = it.next();
					shuttingVMs.add(vm);
					it.remove();
					sendNow(datacenter.getId(), TERMINATE_VM, vm);
				}
			}	
		}
	}
	
	
	/**
	 * This datacenter client adds functionality of dynamically provisioning VMs 
	 * 
	 * @author malawski
	 *
	 */
	private class DatacenterProvisionerDAGClient extends DatacenterDAGClient {
		
		public DatacenterProvisionerDAGClient(String name) {
			super(name);
		}
	
		protected void provisionVMs() {
			
			// provisioning
			if (job.getEligibleCloudlets().size() > freeVMs.size() + submittedVMs.size()) {
				ArrayList<Vm> vmlist = VmListGenerator.generateVmList(1, getId());
				submitVMs(new HashSet<Vm>(vmlist));
			}
			
		}
		
		protected void scheduleCloudlets() {
			
			provisionVMs();
			
			// if there is nothing to do, just return
			if (freeVMs.isEmpty()) return;
			if (newCloudlets.isEmpty()) return;
			Iterator<Cloudlet> cloudletIT = job.getEligibleCloudlets().iterator();
			Iterator<Vm> vmIt = freeVMs.iterator();
			while (cloudletIT.hasNext() && vmIt.hasNext()) {
				Cloudlet cloudlet = cloudletIT.next();
				Vm vm = vmIt.next();
				cloudlet.setVmId(vm.getId());
				vmIt.remove(); // remove VM from free set
				busyVMs.add(vm);
				cloudletIT.remove(); // remove cloudlet from new set
				submittedCloudlets.add(cloudlet);
				sendNow(datacenter.getId(), CLOUDLET_SUBMIT, cloudlet);
				job.setUneligible(cloudlet);
			}		
		}	
	}
	
	
	
	
	/**
	 * This datacenter client schedules DAGs by submitting only eligible cloudlets 
	 * (the ones which have dependencies satisfied) to VMs.
	 * 
	 * @author malawski
	 *
	 */
	private class DatacenterDAGClient extends DatacenterClient {

		protected Job job;
		
		public DatacenterDAGClient(String name) {
			super(name);
		}

		public void setJob(Job job) {
			this.job = job;
		}
		
		protected void scheduleCloudlets() {
			
			// if there is nothing to do, just return
			if (freeVMs.isEmpty()) return;
			if (newCloudlets.isEmpty()) return;
			Iterator<Cloudlet> cloudletIT = job.getEligibleCloudlets().iterator();
			Iterator<Vm> vmIt = freeVMs.iterator();
			while (cloudletIT.hasNext() && vmIt.hasNext()) {
				Cloudlet cloudlet = cloudletIT.next();
				Vm vm = vmIt.next();
				cloudlet.setVmId(vm.getId());
				vmIt.remove(); // remove VM from free set
				busyVMs.add(vm);
				cloudletIT.remove(); // remove cloudlet from new set
				submittedCloudlets.add(cloudlet);
				sendNow(datacenter.getId(), CLOUDLET_SUBMIT, cloudlet);
				job.setUneligible(cloudlet);
			}		
		}
		
		
		protected void completeCloudlet(Cloudlet cloudlet) {
	    	Vm vm = vmids.get(cloudlet.getVmId());
	    	busyVMs.remove(vm);
	    	freeVMs.add(vm);
	    	runningCloudlets.remove(cloudlet);
	    	completedCloudlets.add(cloudlet);
			job.processCloudletReturn(cloudlet);
	    	scheduleCloudlets();
		}
		
		
	}
	
	
	/**
	 * The client class which acts as workflow engine. This one does not handle any dependencies, 
	 * just submits cloudlets to free VMs.
	 * 
	 * @author malawski
	 *
	 */
	private class DatacenterClient extends SimEntity implements WorkflowEvent {

		protected Datacenter datacenter;
		/** The set of VMs. */
		private Set<Vm> vms;
		
		/** The set of submitted VMs (not running yet) */
		protected Set<Vm> submittedVMs;
		
		/** The set of VMs that are running */
		protected Set<Vm> runningVMs;
		
		/** The set of free VMs, i.e. the ones which are not executing any cloudlets (idle) */
		protected Set<Vm> freeVMs;
		
		/** The set of busy VMs, i.e. the ones which execute cloudlets */
		protected Set<Vm> busyVMs;
		
		/** The set of VMs that are shutting down */
		protected Set<Vm> shuttingVMs;
		
		/** The set of terminated VMs */
		protected Set<Vm> terminatedVMs;

		/** The set of new (unsubmitted) cloudlets */
		protected Set<Cloudlet> newCloudlets;
		
		/** The set of submitted cloudlets */
		protected Set<Cloudlet> submittedCloudlets;
		
		/** The set of running cloudlets */
		protected Set<Cloudlet> runningCloudlets;
		
		/** The set of completed cloudlets */
		protected Set<Cloudlet> completedCloudlets;
		

		protected List<Cloudlet> cloudlets;
		
		/** map from ids to VM objects - probably should be moved to a global scope */
		protected Map<Integer,Vm> vmids;
		
		public DatacenterClient(String name) {
			super(name);
			CloudSim.addEntity(this);
			cloudlets = new ArrayList<Cloudlet>(0);
			runningVMs = new HashSet<Vm>();
	        freeVMs = new HashSet<Vm>();
	        busyVMs = new HashSet<Vm>();
	        submittedVMs = new HashSet<Vm>();
	        shuttingVMs = new HashSet<Vm>();
	        terminatedVMs = new HashSet<Vm>();
	        newCloudlets = new HashSet<Cloudlet>();
	        submittedCloudlets = new HashSet<Cloudlet>();
	        runningCloudlets = new HashSet<Cloudlet>();
	        completedCloudlets = new HashSet<Cloudlet>();
	        vmids = new HashMap<Integer, Vm>();
		}
		
		public Set<Cloudlet> getCompletedCloudlets() {
			return completedCloudlets;
		}
		
		public List<Cloudlet> getCloudlets() {
			return cloudlets;
		}

		public void setCloudlets(List<Cloudlet> cloudlets) {
			this.cloudlets = cloudlets;
		}

		public void setVMs(Set<Vm> vms) {
			this.vms = vms;
		}

		public void setDatacenter(Datacenter datacenter) {
			this.datacenter = datacenter;
		}
		
		public Set<Vm> getRunningVMs() {
			return runningVMs;
		}
		
		public Set<Vm> getTerminatedVMs() {
			return terminatedVMs;
		}

		@Override
		public void startEntity() {
			submitVMs(vms);
			newCloudlets.addAll(cloudlets);
			scheduleCloudlets();
		}
		
		
		protected void submitVMs (Set<Vm> vms) {
			for (Vm vm : vms) {
				send(datacenter.getId(), 0.0, NEW_VM, vm);
				vmids.put(vm.getId(), vm);
				submittedVMs.add(vm);
			}
		}

		protected void scheduleCloudlets() {
			
			// if there is nothing to do, just return
			if (freeVMs.isEmpty()) return;
			if (newCloudlets.isEmpty()) return;
			Iterator<Cloudlet> cloudletIT = newCloudlets.iterator();
			Iterator<Vm> vmIt = freeVMs.iterator();
			while (cloudletIT.hasNext() && vmIt.hasNext()) {
				Cloudlet cloudlet = cloudletIT.next();
				Vm vm = vmIt.next();
				cloudlet.setVmId(vm.getId());
				vmIt.remove(); // remove VM from free set
				busyVMs.add(vm);
				cloudletIT.remove(); // remove cloudlet from new set
				submittedCloudlets.add(cloudlet);
				sendNow(datacenter.getId(), CLOUDLET_SUBMIT, cloudlet);
			}		
		}
		
		

		@Override
		public void processEvent(SimEvent ev) { 
	        switch(ev.getTag()) {
	        case VM_CREATION_COMPLETE:
	        	Vm vm = (Vm)ev.getData();
	            runningVMs.add(vm);
	            freeVMs.add(vm);
	            submittedVMs.remove(vm);
	            scheduleCloudlets();
	            break;
	        case CLOUDLET_STARTED:
	        	startCloudlet((Cloudlet)ev.getData());
	            break;
	        case CLOUDLET_COMPLETE:
	        	completeCloudlet((Cloudlet)ev.getData());
	            break;
	        case VM_TERMINATION_COMPLETE:
	        	vmTerminationComplete((Vm)ev.getData());
	        	break;
	        default:
	            throw new RuntimeException("Unknown event: "+ev);
	        }
		}

		protected void vmTerminationComplete(Vm vm) {
			shuttingVMs.remove(vm);
			runningVMs.remove(vm);
			terminatedVMs.add(vm);
		}

		protected void startCloudlet(Cloudlet cloudlet) {
			Log.printLine(CloudSim.clock() + " Cloudlet started " + cloudlet.getCloudletId());
			submittedCloudlets.remove(cloudlet);
			runningCloudlets.add(cloudlet);
		}
		
		protected void completeCloudlet(Cloudlet cloudlet) {
	    	Vm vm = vmids.get(cloudlet.getVmId());
	    	busyVMs.remove(vm);
	    	freeVMs.add(vm);
	    	runningCloudlets.remove(cloudlet);
	    	completedCloudlets.add(cloudlet);
	    	scheduleCloudlets();
		}

		@Override
		public void shutdownEntity() {	}
		
	}
	
    @Test
    public void testDatacenterVMs() {
        CloudSim.init(1, null, false);
        
        DatacenterClient datacenterClient = new DatacenterClient("Client");
        Datacenter datacenter  = new Datacenter("Datacenter");
        
        datacenterClient.setDatacenter(datacenter);
        
        List<Vm> listVMs = VmListGenerator.generateVmList(1, datacenterClient.getId());
              
        datacenterClient.setVMs(new HashSet<Vm>(listVMs));
        
        CloudSim.startSimulation();   
        
        assertEquals(listVMs.size(), datacenterClient.getRunningVMs().size());
        assertEquals(listVMs.size(), datacenterClient.getRunningVMs().size());
       
    }
    


	@Test
    public void testDatacenterVMs2() {
        CloudSim.init(1, null, false);
        
        DatacenterClient datacenterClient = new DatacenterClient("Client");
        Datacenter datacenter  = new Datacenter("Datacenter");
        
        datacenterClient.setDatacenter(datacenter);
        
        List<Vm> listVMs = VmListGenerator.generateVmList(10, datacenterClient.getId());
              
        datacenterClient.setVMs(new HashSet<Vm>(listVMs));
        
        CloudSim.startSimulation();
        
        assertEquals(listVMs.size(), datacenterClient.getRunningVMs().size());
        
    }
    
    @Test
    public void testDatacenterCloudlets() {
        CloudSim.init(1, null, false);
        
        DatacenterClient datacenterClient = new DatacenterClient("Client");
        Datacenter datacenter  = new Datacenter("Datacenter");
        
        datacenterClient.setDatacenter(datacenter);
        
        List<Vm> listVMs = VmListGenerator.generateVmList(10, datacenterClient.getId());
        List<Cloudlet> cloudlets = CloudletListGenerator.generateCloudlets(12, 1, 600000, 300, 300, datacenterClient.getId()); 
        
        datacenterClient.setVMs(new HashSet<Vm>(listVMs));
        datacenterClient.setCloudlets(cloudlets);
        
        CloudSim.startSimulation();
        
        assertEquals(listVMs.size(), datacenterClient.getRunningVMs().size());      
        assertEquals(cloudlets.size(), datacenterClient.getCompletedCloudlets().size());
        
        Helper.printCloudletList(cloudlets, "testDatacenterCloudlets");
        
    }

    
    @Test
    public void testDatacenterDAG() {
        CloudSim.init(1, null, false);
        
        DatacenterDAGClient datacenterClient = new DatacenterDAGClient("Client");
        Datacenter datacenter  = new Datacenter("Datacenter");
        
        datacenterClient.setDatacenter(datacenter);
        
        List<Vm> listVMs = VmListGenerator.generateVmList(10, datacenterClient.getId());
        List<Cloudlet> cloudlets = CloudletListGenerator.generateCloudlets(120, 1, 600000, 300, 300, datacenterClient.getId()); 

    	Job job = new Job();
    	job.setCloudlets(cloudlets);
    	job.generateDag();

        
        datacenterClient.setVMs(new HashSet<Vm>(listVMs));
        datacenterClient.setCloudlets(cloudlets);
        datacenterClient.setJob(job);
        
        
        CloudSim.startSimulation();
        
        assertEquals(listVMs.size(), datacenterClient.getRunningVMs().size());
        assertEquals(cloudlets.size(), datacenterClient.getCompletedCloudlets().size());
        
        Helper.printCloudletList(cloudlets, "testDatacenterDAG");
        
    }
    

    
    
    
    //@Test
    public void testDatacenterReadDAG() {
        CloudSim.init(1, null, false);
        
        DatacenterDAGClient datacenterClient = new DatacenterDAGClient("Client");
        Datacenter datacenter  = new Datacenter("Datacenter");
        
        datacenterClient.setDatacenter(datacenter);
        
        List<Vm> listVMs = VmListGenerator.generateVmList(256, datacenterClient.getId());

    	Job job = new Job();
    	job.readDag(datacenterClient.getId(), "dags/cybershake_small.dag");
        
        datacenterClient.setVMs(new HashSet<Vm>(listVMs));
        datacenterClient.setCloudlets(job.getCloudlets());
        datacenterClient.setJob(job);
        
        
        CloudSim.startSimulation();
        
        assertEquals(listVMs.size(), datacenterClient.getRunningVMs().size());
        assertEquals(job.getCloudlets().size(), datacenterClient.getCompletedCloudlets().size());
        
        Helper.printCloudletList(job.getCloudlets(), "testDatacenterReadDAG");
        
    }
    
    //@Test
    public void testDatacenterProvisionerDAG() {
        CloudSim.init(1, null, false);
        
        DatacenterProvisionerDAGClient datacenterClient = new DatacenterProvisionerDAGClient("Client");
        Datacenter datacenter  = new Datacenter("Datacenter");
        
        datacenterClient.setDatacenter(datacenter);
        
        List<Vm> listVMs = VmListGenerator.generateVmList(1, datacenterClient.getId());

    	Job job = new Job();
    	job.readDag(datacenterClient.getId(), "dags/cybershake_small.dag");
        
        datacenterClient.setVMs(new HashSet<Vm>(listVMs));
        datacenterClient.setCloudlets(job.getCloudlets());
        datacenterClient.setJob(job);
        
        
        CloudSim.startSimulation();
        
        assertEquals(job.getCloudlets().size(), datacenterClient.getCompletedCloudlets().size());
        
        Helper.printCloudletList(job.getCloudlets(), "testDatacenterProvisionerDAG");
        
    }
    
    
	public void runDatacenterDeprovisioner(String dagPath, String outputName) {

        CloudSim.init(1, null, false);
        
		// try {
		// Log.setOutput(new FileOutputStream(new
		// File("testDatacenterDeprovisionerDAG.log")));
		// } catch (FileNotFoundException e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// }

		DatacenterDeprovisionerDAGClient datacenterClient = new DatacenterDeprovisionerDAGClient("Client");
		Datacenter datacenter = new Datacenter("Datacenter");

		datacenterClient.setDatacenter(datacenter);

		List<Vm> listVMs = VmListGenerator.generateVmList(1, datacenterClient.getId());

		Job job = new Job();
		job.readDag(datacenterClient.getId(), dagPath);

		datacenterClient.setVMs(new HashSet<Vm>(listVMs));
		datacenterClient.setCloudlets(job.getCloudlets());
		datacenterClient.setJob(job);

		CloudSim.startSimulation();

		assertEquals(0, datacenterClient.getRunningVMs().size());
		assertEquals(job.getCloudlets().size(), datacenterClient.getCompletedCloudlets().size());

		Helper.printCloudletList(job.getCloudlets(), outputName);
		Helper.printVmList(datacenter.getVmCreationTimes(), datacenter.getVmTerminationTimes(), outputName);
	}
    
    
    //@Test
    public void testDatacenterDeprovisionerDAG() {
        runDatacenterDeprovisioner("dags/cybershake_small.dag", "testDatacenterDeprovisionerDAG");
    }
    
    
    @Test
    public void testDatacenterDeprovisioner30DAG() {
    	runDatacenterDeprovisioner("dags/CyberShake_30.dag", "testDatacenterDeprovisioner30DAG");
    }

    @Test
    public void testDatacenterDeprovisioner50DAG() {
    	runDatacenterDeprovisioner("dags/CyberShake_50.dag", "testDatacenterDeprovisioner50DAG");
    }
    
    @Test
    public void testDatacenterDeprovisioner100DAG() {
    	runDatacenterDeprovisioner("dags/CyberShake_100.dag", "testDatacenterDeprovisioner100DAG");        
    }
    
    @Test
    public void testDatacenterDeprovisioner1000DAG() {
    	runDatacenterDeprovisioner("dags/CyberShake_1000.dag", "testDatacenterDeprovisioner1000DAG");        
    }
       
}
