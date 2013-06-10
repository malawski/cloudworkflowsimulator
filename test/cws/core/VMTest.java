package cws.core;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Collections;

import org.junit.Before;
import org.junit.Test;

import cws.core.cloudsim.CWSSimEntity;
import cws.core.cloudsim.CWSSimEvent;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.dag.DAGFile;
import cws.core.dag.Task;
import cws.core.jobs.Job;
import cws.core.storage.StorageManager;
import cws.core.storage.VoidStorageManager;
import cws.core.storage.cache.FIFOCacheManager;
import cws.core.storage.cache.VMCacheManager;
import cws.core.storage.cache.VoidCacheManager;
import cws.core.storage.global.GlobalStorageManager;
import cws.core.storage.global.GlobalStorageParams;

public class VMTest {

    private CloudSimWrapper cloudsim;

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

        @Override
        public void shutdownEntity() {
        }
    }

    @Before
    public void setUp() {
        // TODO(_mequrel_): change to IoC in the future
        cloudsim = new CloudSimWrapper();
        cloudsim.init();
    }

    @Test
    public void testSingleJob() {
        // TODO(bryk): that's ugly, I know
        new VoidStorageManager(cloudsim);

        Job job = new Job(cloudsim);
        Task task = new Task("task_id", "transformation", 1000);
        task.setInputFiles(Collections.EMPTY_LIST);
        task.setOutputFiles(Collections.EMPTY_LIST);
        job.setTask(task);

        VMStaticParams vmStaticParams = new VMStaticParams();
        vmStaticParams.setMips(100);
        vmStaticParams.setCores(1);
        vmStaticParams.setPrice(0.40);

        VM vm = new VM(100, vmStaticParams, cloudsim);

        VMDriver driver = new VMDriver(vm, cloudsim);
        driver.setJobs(new Job[] { job });

        cloudsim.startSimulation();

        assertEquals(0.0, job.getReleaseTime(), 0.0);
        assertEquals(0.0, job.getSubmitTime(), 0.0);
        assertEquals(0.0, job.getStartTime(), 0.0);
        assertEquals(10.0, job.getFinishTime(), 0.0);
    }

    @Test
    public void testTwoJobs() {
        // TODO(bryk): that's ugly, I know
        new VoidStorageManager(cloudsim);

        Job j1 = new Job(cloudsim);
        Task task1 = new Task("task_id", "transformation", 1000);
        task1.setInputFiles(Collections.EMPTY_LIST);
        task1.setOutputFiles(Collections.EMPTY_LIST);
        j1.setTask(task1);

        Job j2 = new Job(cloudsim);
        Task task2 = new Task("task_id2", "transformation", 1000);
        task2.setInputFiles(Collections.EMPTY_LIST);
        task2.setOutputFiles(Collections.EMPTY_LIST);
        j2.setTask(task2);

        VMStaticParams vmStaticParams = new VMStaticParams();
        vmStaticParams.setMips(100);
        vmStaticParams.setCores(1);
        vmStaticParams.setPrice(0.40);

        VM vm = new VM(100, vmStaticParams, cloudsim);

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
        // TODO(bryk): that's ugly, I know
        new VoidStorageManager(cloudsim);

        Job j1 = new Job(cloudsim);
        Task task1 = new Task("task_id", "transformation", 1000);
        task1.setInputFiles(Collections.EMPTY_LIST);
        task1.setOutputFiles(Collections.EMPTY_LIST);
        j1.setTask(task1);

        Job j2 = new Job(cloudsim);
        Task task2 = new Task("task_id2", "transformation", 1000);
        task2.setInputFiles(Collections.EMPTY_LIST);
        task2.setOutputFiles(Collections.EMPTY_LIST);
        j2.setTask(task2);

        VMStaticParams vmStaticParams = new VMStaticParams();
        vmStaticParams.setMips(100);
        vmStaticParams.setCores(2);
        vmStaticParams.setPrice(0.40);

        VM vm = new VM(100, vmStaticParams, cloudsim);

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
    public void shouldStartNextTaskWithInputFilesAfterUploadingPreviousOutputs() {
        double writeSpeed = 60;
        double readSpeed = 60;

        GlobalStorageParams params = new GlobalStorageParams();
        params.setReadSpeed(readSpeed);
        params.setWriteSpeed(writeSpeed);

        StorageManager storageManager = new GlobalStorageManager(params, new VoidCacheManager(cloudsim), cloudsim);
        ResourceLocator.setStorageManager(storageManager);

        // first task

        double firstLength = 10.0;
        Job jobFirst = new Job(cloudsim);
        Task taskWithOutputFile = new Task(null, null, firstLength);

        int outputFileSizeInBytes = 300;
        DAGFile file = new DAGFile("output-file", outputFileSizeInBytes);

        taskWithOutputFile.setInputFiles(Collections.EMPTY_LIST);
        taskWithOutputFile.setOutputFiles(Arrays.asList(new DAGFile[] { file }));
        jobFirst.setTask(taskWithOutputFile);

        // second task

        double secondLength = 10.0;
        Job jobSecond = new Job(cloudsim);
        Task taskWithInputFile = new Task(null, null, secondLength);

        int inputFileSizeInBytes = 300;
        DAGFile inputFile = new DAGFile("input-file", inputFileSizeInBytes);

        taskWithInputFile.setInputFiles(Arrays.asList(new DAGFile[] { inputFile }));
        taskWithInputFile.setOutputFiles(Collections.EMPTY_LIST);
        jobSecond.setTask(taskWithInputFile);

        // create VM

        VMStaticParams vmStaticParams = new VMStaticParams();
        vmStaticParams.setMips(1);
        vmStaticParams.setCores(1);
        vmStaticParams.setPrice(0.40);

        VM vm = new VM(100, vmStaticParams, cloudsim);

        // prepare tasks

        jobFirst.setVM(vm);
        jobSecond.setVM(vm);

        // send tasks

        VMDriver driver = new VMDriver(vm, cloudsim);
        driver.setJobs(new Job[] { jobFirst, jobSecond });

        // expectations
        double expectedFirstEnded = firstLength;
        double expectedSecondStarted = expectedFirstEnded + outputFileSizeInBytes / writeSpeed;
        ;
        double expectedSecondEnded = expectedSecondStarted + secondLength + inputFileSizeInBytes / writeSpeed;

        // simulate

        cloudsim.startSimulation();

        // assert times -> cannot be parallelised, everything runs subsequently

        assertEquals(expectedFirstEnded, jobFirst.getFinishTime(), 0.0);
        assertEquals(expectedSecondStarted, jobSecond.getStartTime(), 0.0);
        assertEquals(expectedSecondEnded, jobSecond.getFinishTime(), 0.0);
    }

    @Test
    public void shouldStartNextTaskWithoutInputFilesImmediately() {
        double writeSpeed = 60;
        double readSpeed = 60;

        GlobalStorageParams params = new GlobalStorageParams();
        params.setReadSpeed(readSpeed);
        params.setWriteSpeed(writeSpeed);

        StorageManager storageManager = new GlobalStorageManager(params, new VoidCacheManager(cloudsim), cloudsim);
        ResourceLocator.setStorageManager(storageManager);

        // first task

        double firstLength = 10.0;
        Job jobFirst = new Job(cloudsim);
        Task taskWithOutputFile = new Task(null, null, firstLength);

        int outputFileSizeInBytes = 300;
        DAGFile file = new DAGFile("input-file", outputFileSizeInBytes);

        taskWithOutputFile.setInputFiles(Collections.EMPTY_LIST);
        taskWithOutputFile.setOutputFiles(Arrays.asList(new DAGFile[] { file }));
        jobFirst.setTask(taskWithOutputFile);

        // second task

        double secondLength = 10.0;
        Job jobSecond = new Job(cloudsim);
        Task taskWithoutInputFile = new Task(null, null, secondLength);
        taskWithoutInputFile.setInputFiles(Collections.EMPTY_LIST);
        taskWithoutInputFile.setOutputFiles(Collections.EMPTY_LIST);
        jobSecond.setTask(taskWithoutInputFile);

        // create VM

        VMStaticParams vmStaticParams = new VMStaticParams();
        vmStaticParams.setMips(1);
        vmStaticParams.setCores(1);
        vmStaticParams.setPrice(0.40);

        VM vm = new VM(100, vmStaticParams, cloudsim);

        // prepare tasks

        jobFirst.setVM(vm);
        jobSecond.setVM(vm);

        // send tasks

        VMDriver driver = new VMDriver(vm, cloudsim);
        driver.setJobs(new Job[] { jobFirst, jobSecond });

        // expectations
        double expectedFirstEnded = firstLength;
        double expectedSecondStarted = firstLength;
        double expectedSecondEnded = firstLength + secondLength;

        // simulate

        cloudsim.startSimulation();

        // assert that overall time is exec1 + exec2 only

        assertEquals(expectedFirstEnded, jobFirst.getFinishTime(), 0.0);
        assertEquals(expectedSecondStarted, jobSecond.getStartTime(), 0.0);
        assertEquals(expectedSecondEnded, jobSecond.getFinishTime(), 0.0);
    }

    @Test
    public void shouldStartNextTaskWithCashedInputFilesImmediately() {
        double writeSpeed = 60;
        double readSpeed = 60;

        GlobalStorageParams params = new GlobalStorageParams();
        params.setReadSpeed(readSpeed);
        params.setWriteSpeed(writeSpeed);

        VMCacheManager cacheManager = new FIFOCacheManager(cloudsim);
        StorageManager storageManager = new GlobalStorageManager(params, cacheManager, cloudsim);
        ResourceLocator.setStorageManager(storageManager);

        // first task

        double firstLength = 10.0;
        Job jobFirst = new Job(cloudsim);
        Task taskWithOutputFile = new Task(null, null, firstLength);

        int outputFileSizeInBytes = 300;
        DAGFile file = new DAGFile("input-file", outputFileSizeInBytes);

        taskWithOutputFile.setInputFiles(Collections.EMPTY_LIST);
        taskWithOutputFile.setOutputFiles(Arrays.asList(new DAGFile[] { file }));
        jobFirst.setTask(taskWithOutputFile);

        // second task

        double secondLength = 10.0;
        Job jobSecond = new Job(cloudsim);
        Task taskWithoutInputFile = new Task(null, null, secondLength);
        taskWithoutInputFile.setInputFiles(Arrays.asList(new DAGFile[] { file }));
        taskWithoutInputFile.setOutputFiles(Collections.EMPTY_LIST);
        jobSecond.setTask(taskWithoutInputFile);

        // create VM

        VMStaticParams vmStaticParams = new VMStaticParams();
        vmStaticParams.setMips(1);
        vmStaticParams.setCores(1);
        vmStaticParams.setPrice(0.40);

        VM vm = new VM(100, vmStaticParams, cloudsim);

        vm.setCacheSize(10000000000000L);

        // prepare tasks

        jobFirst.setVM(vm);
        jobSecond.setVM(vm);

        // send tasks

        VMDriver driver = new VMDriver(vm, cloudsim);
        driver.setJobs(new Job[] { jobFirst, jobSecond });

        // expectations
        double expectedFirstEnded = firstLength;
        double expectedSecondStarted = firstLength;
        double expectedSecondEnded = firstLength + secondLength;

        // simulate

        cloudsim.startSimulation();

        // assert that overall time is exec1 + exec2 only

        assertEquals(expectedFirstEnded, jobFirst.getFinishTime(), 0.0);
        assertEquals(expectedSecondStarted, jobSecond.getStartTime(), 0.0);
        assertEquals(expectedSecondEnded, jobSecond.getFinishTime(), 0.0);
    }

}
