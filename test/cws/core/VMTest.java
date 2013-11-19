package cws.core;

import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;

import cws.core.cloudsim.CWSSimEntity;
import cws.core.cloudsim.CWSSimEvent;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.core.VMType;
import cws.core.core.VMTypeBuilder;
import cws.core.core.VMTypeFactory;
import cws.core.dag.DAG;
import cws.core.dag.DAGJob;
import cws.core.dag.Task;
import cws.core.jobs.Job;
import cws.core.storage.StorageManager;
import cws.core.storage.VoidStorageManager;

public class VMTest {
    private CloudSimWrapper cloudsim;
    @SuppressWarnings("unused")
    private StorageManager storageManager;

    private VMType testDefaultVMType;

    private class VMDriver extends CWSSimEntity {
        private VM vm;
        private Job[] jobs;

        public VMDriver(VM vm, CloudSimWrapper cloudsim) {
            super("VMDriver", cloudsim);
            this.vm = vm;
            getCloudsim().addEntity(this);
        }

        public void setJobs(Job[] jobs) {
            this.jobs = jobs;
        }

        @Override
        public void startEntity() {
            sendNow(vm.getId(), WorkflowEvent.VM_LAUNCH);

            // Submit all the jobs
            for (Job j : jobs) {
                j.setOwner(getId());
                getCloudsim().send(getId(), vm.getId(), 0.0, WorkflowEvent.JOB_SUBMIT, j);
            }
        }

        @Override
        public void processEvent(CWSSimEvent ev) {
            switch (ev.getTag()) {
            case WorkflowEvent.JOB_STARTED: {
                Job j = (Job) ev.getData();
                assertEquals(Job.State.RUNNING, j.getState());
                break;
            }
            case WorkflowEvent.JOB_FINISHED: {
                Job j = (Job) ev.getData();
                assertEquals(Job.State.TERMINATED, j.getState());
                break;
            }
            }
        }
    }

    private class VMDummyDriver extends CWSSimEntity {
        private VM vm;

        public VMDummyDriver(VM vm, CloudSimWrapper cloudsim) {
            super("VMDummyDriver", cloudsim);
            this.vm = vm;
            getCloudsim().addEntity(this);
        }
    }

    @Before
    public void setUp() {
        cloudsim = new CloudSimWrapper();
        cloudsim.init();
        storageManager = new VoidStorageManager(cloudsim);
        testDefaultVMType = new VMTypeBuilder().mips(1000).cores(1).price(1.0).build();
    }

    @Test
    public void testSingleJob() {
        Job j = new Job(cloudsim);
        j.setTask(new Task("task_id", "transformation", 1000, cws.core.algorithms.VMType.DEFAULT_VM_TYPE));
        j.setDAGJob(new DAGJob(new DAG(), 1));

        VMType vmType = new VMTypeBuilder().mips(100).cores(1).price(0.40).build();

        VM vm = new VM(vmType, cloudsim);

        VMDriver driver = new VMDriver(vm, cloudsim);
        driver.setJobs(new Job[] { j });

        cloudsim.startSimulation();

        assertEquals(0.0, j.getReleaseTime(), 0.0);
        assertEquals(0.0, j.getSubmitTime(), 0.0);
        assertEquals(0.0, j.getStartTime(), 0.0);
        assertEquals(10.0, j.getFinishTime(), 0.0);
    }

    @Test
    public void testTwoJobs() {
        Job j1 = new Job(cloudsim);
        j1.setTask(new Task("task_id", "transformation", 1000, cws.core.algorithms.VMType.DEFAULT_VM_TYPE));
        j1.setDAGJob(new DAGJob(new DAG(), 1));
        Job j2 = new Job(cloudsim);
        j2.setTask(new Task("task_id2", "transformation", 1000, cws.core.algorithms.VMType.DEFAULT_VM_TYPE));
        j2.setDAGJob(new DAGJob(new DAG(), 1));

        VMType vmType = new VMTypeBuilder().mips(100).cores(1).price(0.40).build();

        VM vm = new VM(vmType, cloudsim);

        VMDriver driver = new VMDriver(vm, cloudsim);
        driver.setJobs(new Job[] { j1, j2 });

        cloudsim.startSimulation();

        assertEquals(0.0, j1.getReleaseTime(), 0.0);
        assertEquals(0.0, j1.getSubmitTime(), 0.0);
        assertEquals(0.0, j1.getStartTime(), 0.0);
        assertEquals(10.0, j1.getFinishTime(), 0.0);

        assertEquals(0.0, j2.getReleaseTime(), 0.0);
        assertEquals(0.0, j2.getSubmitTime(), 0.0);
        assertEquals(10.0, j2.getStartTime(), 0.0);
        assertEquals(20.0, j2.getFinishTime(), 0.0);
    }

    @Test
    public void testMultiCoreVM() {
        Job j1 = new Job(cloudsim);
        j1.setTask(new Task("task_id1", "transformation", 1000, cws.core.algorithms.VMType.DEFAULT_VM_TYPE));
        j1.setDAGJob(new DAGJob(new DAG(), 1));

        Job j2 = new Job(cloudsim);
        j2.setTask(new Task("task_id2", "transformation", 1000, cws.core.algorithms.VMType.DEFAULT_VM_TYPE));
        j2.setDAGJob(new DAGJob(new DAG(), 1));

        VMType vmType = new VMTypeBuilder().mips(100).cores(2).price(0.40).build();

        VM vm = new VM(vmType, cloudsim);

        VMDriver driver = new VMDriver(vm, cloudsim);
        driver.setJobs(new Job[] { j1, j2 });

        cloudsim.startSimulation();

        assertEquals(0.0, j1.getReleaseTime(), 0.0);
        assertEquals(0.0, j1.getSubmitTime(), 0.0);
        assertEquals(0.0, j1.getStartTime(), 0.0);
        assertEquals(10.0, j1.getFinishTime(), 0.0);

        assertEquals(0.0, j2.getReleaseTime(), 0.0);
        assertEquals(0.0, j2.getSubmitTime(), 0.0);
        assertEquals(0.0, j2.getStartTime(), 0.0);
        assertEquals(10.0, j2.getFinishTime(), 0.0);
    }

    @Test
    public void testVMShouldNotStartAutomatically() {
        VM vm = new VM(testDefaultVMType, cloudsim);
        cloudsim.startSimulation();

        assertEquals(false, vm.isTerminated());
    }

    @Test
    public void testVMShouldStartProperly() {
        VM vm = new VM(testDefaultVMType, cloudsim);
        cloudsim.send(0, vm.getId(), 0.1, WorkflowEvent.VM_LAUNCH);
        cloudsim.startSimulation();

        assertEquals(false, vm.isTerminated());
    }

    @Test
    public void testVMShouldTerminateProperly() {
        VM vm = new VM(testDefaultVMType, cloudsim);
        cloudsim.send(0, vm.getId(), 0.1, WorkflowEvent.VM_LAUNCH);
        cloudsim.send(0, vm.getId(), 0.2, WorkflowEvent.VM_TERMINATE);
        cloudsim.startSimulation();

        assertEquals(true, vm.isTerminated());
    }

    @Test
    public void testVMShouldNotAcceptEventsAfterTermination() {
        VM vm = new VM(testDefaultVMType, cloudsim);
        VMDummyDriver driver = new VMDummyDriver(vm, cloudsim);

        cloudsim.send(driver.getId(), vm.getId(), 0.1, WorkflowEvent.VM_LAUNCH);
        cloudsim.send(driver.getId(), vm.getId(), 0.2, WorkflowEvent.VM_TERMINATE);
        cloudsim.send(driver.getId(), vm.getId(), 0.3, WorkflowEvent.VM_LAUNCH);
        cloudsim.startSimulation();

        assertEquals(true, vm.isTerminated());
    }

    @Test
    public void testVMKillJobsUponTermination() {
        Job job = new Job(cloudsim);
        job.setTask(new Task("task_id1", "transformation", 1000, cws.core.algorithms.VMType.DEFAULT_VM_TYPE));
        job.setDAGJob(new DAGJob(new DAG(), 1));

        VMType vmType = VMTypeFactory.fromOldVMType(cws.core.algorithms.VMType.DEFAULT_VM_TYPE);

        VM vm = new VM(vmType, cloudsim);
        VMDummyDriver driver = new VMDummyDriver(vm, cloudsim);
        job.setOwner(driver.getId());

        cloudsim.send(driver.getId(), vm.getId(), 0.1, WorkflowEvent.VM_LAUNCH);
        cloudsim.send(driver.getId(), vm.getId(), 0.2, WorkflowEvent.JOB_SUBMIT, job);
        cloudsim.send(driver.getId(), vm.getId(), 0.200001, WorkflowEvent.VM_TERMINATE);
        cloudsim.startSimulation();

        assertEquals(Job.Result.FAILURE, job.getResult());
    }

    @Test
    public void testVMShouldNotAcceptNewJobsAfterTermination() {
        Job job2 = new Job(cloudsim);
        job2.setTask(new Task("task_id1", "transformation", 1000, cws.core.algorithms.VMType.DEFAULT_VM_TYPE));
        job2.setDAGJob(new DAGJob(new DAG(), 1));

        VMType vmType = VMTypeFactory.fromOldVMType(cws.core.algorithms.VMType.DEFAULT_VM_TYPE);

        VM vm = new VM(vmType, cloudsim);
        VMDummyDriver driver = new VMDummyDriver(vm, cloudsim);

        cloudsim.send(driver.getId(), vm.getId(), 0.1, WorkflowEvent.VM_LAUNCH);
        cloudsim.send(driver.getId(), vm.getId(), 0.200001, WorkflowEvent.VM_TERMINATE);
        cloudsim.send(driver.getId(), vm.getId(), 0.3, WorkflowEvent.JOB_SUBMIT, job2);
        cloudsim.startSimulation();

        assertEquals(Job.Result.NONE, job2.getResult());
    }
}
