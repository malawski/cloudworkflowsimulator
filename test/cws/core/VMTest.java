package cws.core;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;

import cws.core.cloudsim.CWSSimEntity;
import cws.core.cloudsim.CWSSimEvent;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.core.VMType;
import cws.core.core.VMTypeBuilder;
import cws.core.dag.DAG;
import cws.core.dag.DAGJob;
import cws.core.dag.Task;
import cws.core.engine.Environment;
import cws.core.jobs.Job;
import cws.core.storage.StorageManager;
import cws.core.storage.VoidStorageManager;

public class VMTest {

    private static final double DELTA = 0.01;

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
            vm.launch();

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

    @Test
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
        cloudsim.send(driver.getId(), vm.getId(), 0.15, WorkflowEvent.JOB_SUBMIT, job);
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
        cloudsim.send(driver.getId(), vm.getId(), 0.3, WorkflowEvent.JOB_SUBMIT, job2);
        cloudsim.startSimulation();
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

    @Test
    public void testPredictReleaseTimeWhenNumberOfTasksInQueueIsLessThanNumberOfCores() throws Exception {
        final VMType vmType = VMTypeBuilder.newBuilder().mips(1).cores(4).price(1.0).build();
        final VM vm = VMFactory.createVM(vmType, this.cloudsim);
        final VMDummyDriver driver = new VMDummyDriver(this.cloudsim);
        final Job job = new Job(
                new DAGJob(new DAG(), 1), new Task("task_id1", "transformation", 1000), driver.getId(), this.cloudsim);
        vm.launch();
        vm.jobSubmit(job);
        final StorageManager sm = mock(StorageManager.class);
        when(sm.getTotalTransferTimeEstimation(job.getTask(), vm)).thenReturn(1.0);
        final Environment env = mock(Environment.class);
        when(env.getComputationPredictedRuntime(job.getTask())).thenReturn(2.0);
        assertEquals(0.0, vm.getPredictedReleaseTime(sm, env), DELTA);
    }

    @Test
    public void testPredictReleaseTimeOfSingleCoreVm() throws Exception {
        final VMType vmType = VMTypeBuilder.newBuilder().mips(1).cores(1).price(1.0).build();
        final VM vm = VMFactory.createVM(vmType, this.cloudsim);
        final VMDummyDriver driver = new VMDummyDriver(this.cloudsim);
        final Job job1 = new Job(
                new DAGJob(new DAG(), 1), new Task("task_id1", "transformation", 1000), driver.getId(), this.cloudsim);
        final Job job2 = new Job(
                new DAGJob(new DAG(), 1), new Task("task_id2", "transformation", 1000), driver.getId(), this.cloudsim);
        vm.launch();
        vm.jobSubmit(job1);
        vm.jobSubmit(job2);
        final StorageManager sm = mock(StorageManager.class);
        when(sm.getTotalTransferTimeEstimation(job1.getTask(), vm)).thenReturn(1.0);
        when(sm.getTotalTransferTimeEstimation(job2.getTask(), vm)).thenReturn(2.0);
        final Environment env = mock(Environment.class);
        when(env.getComputationPredictedRuntime(job1.getTask())).thenReturn(2.0);
        when(env.getComputationPredictedRuntime(job2.getTask())).thenReturn(3.0);
        assertEquals(8.0, vm.getPredictedReleaseTime(sm, env), DELTA);
    }

    @Test
    public void testPredictReleaseTimeWhenNumberOfTasksEqualsNumberOfCores() throws Exception {
        final VMType vmType = VMTypeBuilder.newBuilder().mips(1).cores(2).price(1.0).build();
        final VM vm = VMFactory.createVM(vmType, this.cloudsim);
        final VMDummyDriver driver = new VMDummyDriver(this.cloudsim);
        final Job job1 = new Job(
                new DAGJob(new DAG(), 1), new Task("task_id1", "transformation", 1000), driver.getId(), this.cloudsim);
        final Job job2 = new Job(
                new DAGJob(new DAG(), 1), new Task("task_id2", "transformation", 1000), driver.getId(), this.cloudsim);
        vm.launch();
        vm.jobSubmit(job1);
        vm.jobSubmit(job2);
        final StorageManager sm = mock(StorageManager.class);
        when(sm.getTotalTransferTimeEstimation(job1.getTask(), vm)).thenReturn(1.0);
        when(sm.getTotalTransferTimeEstimation(job2.getTask(), vm)).thenReturn(2.0);
        final Environment env = mock(Environment.class);
        when(env.getComputationPredictedRuntime(job1.getTask())).thenReturn(2.0);
        when(env.getComputationPredictedRuntime(job2.getTask())).thenReturn(3.0);
        assertEquals(3.0, vm.getPredictedReleaseTime(sm, env), DELTA);
    }

    @Test
    public void testPredictReleaseTimeOfMultiCoreVm() throws Exception {
        final VMType vmType = VMTypeBuilder.newBuilder().mips(1).cores(2).price(1.0).build();
        final VM vm = VMFactory.createVM(vmType, this.cloudsim);
        final VMDummyDriver driver = new VMDummyDriver(this.cloudsim);
        final Job[] jobs = {
                new Job(new DAGJob(new DAG(), 1), new Task("task_id1", "transformation", 1000), driver.getId(),
                        this.cloudsim),
                new Job(new DAGJob(new DAG(), 1), new Task("task_id2", "transformation", 1000), driver.getId(),
                        this.cloudsim),
                new Job(new DAGJob(new DAG(), 1), new Task("task_id3", "transformation", 1000), driver.getId(),
                        this.cloudsim),
                new Job(new DAGJob(new DAG(), 1), new Task("task_id4", "transformation", 1000), driver.getId(),
                        this.cloudsim),
                new Job(new DAGJob(new DAG(), 1), new Task("task_id5", "transformation", 1000), driver.getId(),
                        this.cloudsim) };
        vm.launch();
        for(final Job job : jobs){
            vm.jobSubmit(job);
        }
        final double[] transferTimes = {1.0, 0.0, 1.0, 0.0, 2.0};
        final double[] computationTimes = {1.0, 2.0, 2.0, 1.0, 2.0};
        final StorageManager sm = mock(StorageManager.class);
        final Environment env = mock(Environment.class);
        for(int i = 0; i < 5; i++) {
            when(sm.getTotalTransferTimeEstimation(jobs[i].getTask(), vm)).thenReturn(transferTimes[i]);
            when(env.getComputationPredictedRuntime(jobs[i].getTask())).thenReturn(computationTimes[i]);
        }
        assertEquals(5.0, vm.getPredictedReleaseTime(sm, env), DELTA);
    }
}
