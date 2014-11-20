package cws.core;

import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import cws.core.cloudsim.CWSSimEntity;
import cws.core.cloudsim.CWSSimEvent;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.core.VMType;
import cws.core.core.VMTypeBuilder;
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
        }

        public void setJobs(Job[] jobs) {
            this.jobs = jobs;
        }

        @Override
        public void startEntity() {
            sendNow(vm.getId(), WorkflowEvent.VM_LAUNCH);

            // Submit all the jobs
            for (Job job : jobs) {
                vm.jobSubmit(job);
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
        public VMDummyDriver(CloudSimWrapper cloudsim) {
            super("VMDummyDriver", cloudsim);
        }
    }

    @Before
    public void setUp() {
        cloudsim = new CloudSimWrapper();
        cloudsim.init();
        storageManager = new VoidStorageManager(cloudsim);
        testDefaultVMType = VMTypeBuilder.newBuilder().mips(1000).cores(1).price(1.0).build();
    }

    @Test
    public void testSingleJob() {
        VMType vmType = VMTypeBuilder.newBuilder().mips(100).cores(1).price(0.40).build();
        VM vm = VMFactory.createVM(vmType, cloudsim);
        VMDriver driver = new VMDriver(vm, cloudsim);

        Job j = new Job(new DAGJob(new DAG(), 1), new Task("task_id", "transformation", 1000), driver.getId(), cloudsim);

        driver.setJobs(new Job[] { j });

        cloudsim.startSimulation();

        assertEquals(0.0, j.getReleaseTime(), 0.0);
        assertEquals(0.0, j.getSubmitTime(), 0.0);
        assertEquals(0.0, j.getStartTime(), 0.0);
        assertEquals(10.0, j.getFinishTime(), 0.0);
    }

    @Test
    public void testTwoJobs() {
        VMType vmType = VMTypeBuilder.newBuilder().mips(100).cores(1).price(0.40).build();
        VM vm = VMFactory.createVM(vmType, cloudsim);
        VMDriver driver = new VMDriver(vm, cloudsim);

        Job j1 = new Job(new DAGJob(new DAG(), 1), new Task("task_id", "transformation", 1000), driver.getId(), cloudsim);
        Job j2 = new Job(new DAGJob(new DAG(), 1), new Task("task_id2", "transformation", 1000), driver.getId(), cloudsim);

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

    /**
     * We cancel support of multicore VMs. Once it is reintrucuded, then we should unignore this.
     */
    @Test
    @Ignore
    public void testMultiCoreVM() {
        VMType vmType = VMTypeBuilder.newBuilder().mips(100).cores(2).price(0.40).build();
        VM vm = VMFactory.createVM(vmType, cloudsim);
        VMDriver driver = new VMDriver(vm, cloudsim);

        Job j1 = new Job(new DAGJob(new DAG(), 1), new Task("task_id1", "transformation", 1000), driver.getId(), cloudsim);
        Job j2 = new Job(new DAGJob(new DAG(), 1), new Task("task_id2", "transformation", 1000), driver.getId(), cloudsim);

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
        VM vm = VMFactory.createVM(testDefaultVMType, cloudsim);
        cloudsim.startSimulation();

        assertEquals(false, vm.isTerminated());
    }

    @Test
    public void testVMShouldStartProperly() {
        VM vm = VMFactory.createVM(testDefaultVMType, cloudsim);
        cloudsim.send(0, vm.getId(), 0.1, WorkflowEvent.VM_LAUNCH);
        cloudsim.startSimulation();

        assertEquals(false, vm.isTerminated());
    }

    @Test
    public void testVMShouldTerminateProperly() {
        VM vm = VMFactory.createVM(testDefaultVMType, cloudsim);
        cloudsim.send(0, vm.getId(), 0.1, WorkflowEvent.VM_LAUNCH);
        cloudsim.send(0, vm.getId(), 0.2, WorkflowEvent.VM_TERMINATE);
        cloudsim.startSimulation();

        assertEquals(true, vm.isTerminated());
    }

    @Test(expected = IllegalStateException.class)
    public void testVMShouldNotAcceptEventsAfterTermination() {
        VM vm = VMFactory.createVM(testDefaultVMType, cloudsim);
        VMDummyDriver driver = new VMDummyDriver(cloudsim);

        cloudsim.send(driver.getId(), vm.getId(), 0.1, WorkflowEvent.VM_LAUNCH);
        cloudsim.send(driver.getId(), vm.getId(), 0.2, WorkflowEvent.VM_TERMINATE);
        cloudsim.send(driver.getId(), vm.getId(), 0.3, WorkflowEvent.VM_LAUNCH);
        cloudsim.startSimulation();
    }

    @Test
    public void testVMKillJobsUponTermination() {
        VMType vmType = VMTypeBuilder.newBuilder().mips(1).cores(1).price(1.0).build();
        VM vm = VMFactory.createVM(vmType, cloudsim);
        VMDummyDriver driver = new VMDummyDriver(cloudsim);

        Job job = new Job(new DAGJob(new DAG(), 1), new Task("task_id1", "transformation", 1000), driver.getId(), cloudsim);

        cloudsim.send(driver.getId(), vm.getId(), 0.1, WorkflowEvent.VM_LAUNCH);
        vm.jobSubmit(job);
        cloudsim.send(driver.getId(), vm.getId(), 0.200001, WorkflowEvent.VM_TERMINATE);
        cloudsim.startSimulation();

        assertEquals(Job.Result.FAILURE, job.getResult());
    }

    @Test(expected = IllegalStateException.class)
    public void testVMShouldNotAcceptNewJobsAfterTermination() {
        VMType vmType = VMTypeBuilder.newBuilder().mips(1).cores(1).price(1.0).build();
        VM vm = VMFactory.createVM(vmType, cloudsim);
        VMDummyDriver driver = new VMDummyDriver(cloudsim);

        Job job2 = new Job(new DAGJob(new DAG(), 1), new Task("task_id1", "transformation", 1000), driver.getId(), cloudsim);

        cloudsim.send(driver.getId(), vm.getId(), 0.1, WorkflowEvent.VM_LAUNCH);
        cloudsim.send(driver.getId(), vm.getId(), 0.200001, WorkflowEvent.VM_TERMINATE);
        cloudsim.startSimulation();
        vm.jobSubmit(job2);
    }

    @Test(expected = IllegalStateException.class)
    public void testLaunchVMTwice() {
        VMType vmType = VMTypeBuilder.newBuilder().mips(1).cores(1).price(1.0).build();

        VM vm = VMFactory.createVM(vmType, cloudsim);
        VMDummyDriver driver = new VMDummyDriver(cloudsim);

        cloudsim.send(driver.getId(), vm.getId(), 0.1, WorkflowEvent.VM_LAUNCH);
        cloudsim.send(driver.getId(), vm.getId(), 0.2, WorkflowEvent.VM_LAUNCH);
        cloudsim.startSimulation();
    }
}
