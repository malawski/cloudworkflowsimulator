package cws.core;

import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.SimEntity;
import org.cloudbus.cloudsim.core.SimEvent;
import org.junit.Test;
import static org.junit.Assert.*;

import cws.core.dag.Task;

public class TestVM {
    
    private class VMDriver extends SimEntity implements WorkflowEvent {
        private VM vm;
        private Job[] jobs;
        
        public VMDriver(VM vm) {
            super("VMDriver");
            this.vm = vm;
            CloudSim.addEntity(this);
        }
        
        public void setJobs(Job[] jobs) {
            this.jobs = jobs;
        }
        
        @Override
        public void startEntity() {
            sendNow(vm.getId(), VM_LAUNCH);
            
            // Submit all the jobs
            for (Job j: jobs) {
                j.setOwner(getId());
                send(vm.getId(), 0.0, JOB_SUBMIT, j);
            }
        }
        
        @Override
        public void processEvent(SimEvent ev) { 
            switch(ev.getTag()) {
                case JOB_STARTED: {
                    Job j = (Job)ev.getData();
                    assertEquals(Job.State.RUNNING, j.getState());
                    break;
                }
                case JOB_FINISHED: {
                    Job j = (Job)ev.getData();
                    assertEquals(Job.State.TERMINATED, j.getState());
                    break;
                }
            }
        }
        
        @Override
        public void shutdownEntity() { }
    }
    
    @Test
    public void testSingleJob() {
        CloudSim.init(1, null, false);
        
        Task t = new Task("a", "a", 1000);
        Job j = new Job(null, t, 0);
        
        VM vm = new VM(100, 1, 100, 0.40);
        
        VMDriver driver = new VMDriver(vm);
        driver.setJobs(new Job[]{j});
        
        CloudSim.startSimulation();
        
        assertEquals(j.getReleaseTime(), 0.0, 0.0);
        assertEquals(j.getSubmitTime(), 0.0, 0.0);
        assertEquals(j.getStartTime(), 0.0, 0.0);
        assertEquals(j.getFinishTime(), 10.0, 0.0);
    }
    
    @Test
    public void testTwoJobs() {
        CloudSim.init(1, null, false);
        
        Task t = new Task("a", "a", 1000);
        Job j1 = new Job(null, t, 0);
        Job j2 = new Job(null, t, 0);
        
        VM vm = new VM(100, 1, 100, 0.40);
        
        VMDriver driver = new VMDriver(vm);
        driver.setJobs(new Job[]{j1, j2});
        
        CloudSim.startSimulation();
        
        assertEquals(j1.getReleaseTime(), 0.0, 0.0);
        assertEquals(j1.getSubmitTime(), 0.0, 0.0);
        assertEquals(j1.getStartTime(), 0.0, 0.0);
        assertEquals(j1.getFinishTime(), 10.0, 0.0);
        
        assertEquals(j2.getReleaseTime(), 0.0, 0.0);
        assertEquals(j2.getSubmitTime(), 0.0, 0.0);
        assertEquals(j2.getStartTime(), 10.0, 0.0);
        assertEquals(j2.getFinishTime(), 20.0, 0.0);
    }
    
    @Test
    public void testMultiCoreVM() {
        CloudSim.init(1, null, false);
        
        Task t = new Task("a", "a", 1000);
        Job j1 = new Job(null, t, 0);
        Job j2 = new Job(null, t, 0);
        
        VM vm = new VM(100, 2, 100, 0.40);
        
        VMDriver driver = new VMDriver(vm);
        driver.setJobs(new Job[]{j1, j2});
        
        CloudSim.startSimulation();
        
        assertEquals(j1.getReleaseTime(), 0.0, 0.0);
        assertEquals(j1.getSubmitTime(), 0.0, 0.0);
        assertEquals(j1.getStartTime(), 0.0, 0.0);
        assertEquals(j1.getFinishTime(), 10.0, 0.0);
        
        assertEquals(j2.getReleaseTime(), 0.0, 0.0);
        assertEquals(j2.getSubmitTime(), 0.0, 0.0);
        assertEquals(j2.getStartTime(), 0.0, 0.0);
        assertEquals(j2.getFinishTime(), 10.0, 0.0);
    }
}
