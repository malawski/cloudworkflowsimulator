package cws.core.storage;

import java.util.Random;

import junit.framework.Assert;

import org.cloudbus.cloudsim.core.CloudSim;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableList;

import cws.core.VM;
import cws.core.WorkflowEvent;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.dag.DAGFile;
import cws.core.dag.Task;
import cws.core.exception.UnknownWorkflowEventException;
import cws.core.jobs.Job;

/**
 * Tests {@link StorageManager}. An abstract class - should be subclassed and field {@link #storageManager} should be
 * initialized with correct subclass.
 */
public abstract class StorageManagerTest {
    protected Random random;
    protected StorageManager storageManager;
    protected CloudSimWrapper cloudsim;
    protected Job job;
    protected VM vm;
    protected Task task;

    @Before
    public void setUpStorageManagerTest() {
        cloudsim = Mockito.spy(new CloudSimWrapper());
        cloudsim.init();
        random = new Random(7);

        job = Mockito.mock(Job.class);
        vm = Mockito.mock(VM.class);
        Mockito.when(vm.getId()).thenReturn(100);
        Mockito.when(vm.getCloudsim()).thenReturn(cloudsim);
        job.setVM(vm);
        Mockito.when(job.getVM()).thenReturn(vm);
        task = Mockito.mock(Task.class);
        Mockito.when(job.getTask()).thenReturn(task);
    }

    @Test
    public void testEmptySimulation() {
        CloudSim.startSimulation();
    }

    @Test
    public void testBeforeTaskStartOnJobWithNoFiles() {
        Mockito.when(task.getInputFiles()).thenReturn(ImmutableList.<DAGFile>of());
        skipEvent(100, WorkflowEvent.STORAGE_ALL_BEFORE_TRANSFERS_COMPLETED, cloudsim);
        CloudSim.send(-1, storageManager.getId(), random.nextDouble(), WorkflowEvent.STORAGE_BEFORE_TASK_START, job);
        CloudSim.startSimulation();

        Mockito.verify(cloudsim).send(Matchers.anyInt(), Matchers.eq(100), Matchers.anyDouble(),
                Matchers.eq(WorkflowEvent.STORAGE_ALL_BEFORE_TRANSFERS_COMPLETED), Matchers.any());
    }

    @Test
    public void testAfterTaskCompletedOnJobWithNoFiles() {
        Mockito.when(task.getOutputFiles()).thenReturn(ImmutableList.<DAGFile>of());
        skipEvent(100, WorkflowEvent.STORAGE_ALL_AFTER_TRANSFERS_COMPLETED, cloudsim);
        CloudSim.send(-1, storageManager.getId(), random.nextDouble(), WorkflowEvent.STORAGE_AFTER_TASK_COMPLETED, job);
        CloudSim.startSimulation();

        Mockito.verify(cloudsim).send(Matchers.anyInt(), Matchers.eq(100), Matchers.anyDouble(),
                Matchers.eq(WorkflowEvent.STORAGE_ALL_AFTER_TRANSFERS_COMPLETED), Matchers.any());
    }

    @Test
    public void testBeforeTaskStartOnJobWithTwoFiles() {
        ImmutableList<DAGFile> files =
                ImmutableList.of(new DAGFile("abc.txt", 2442, null), new DAGFile("def.txt", 327879, null));
        Mockito.when(task.getInputFiles()).thenReturn(files);
        skipEvent(100, WorkflowEvent.STORAGE_ALL_BEFORE_TRANSFERS_COMPLETED, cloudsim);
        CloudSim.send(-1, storageManager.getId(), random.nextDouble(), WorkflowEvent.STORAGE_BEFORE_TASK_START, job);
        CloudSim.startSimulation();

        Mockito.verify(cloudsim).send(Matchers.anyInt(), Matchers.eq(100), Matchers.anyDouble(),
                Matchers.eq(WorkflowEvent.STORAGE_ALL_BEFORE_TRANSFERS_COMPLETED), Matchers.any());
    }

    @Test
    public void testAfterTaskCompletedOnJobWithTwoFiles() {
        ImmutableList<DAGFile> files =
                ImmutableList.of(new DAGFile("abc.txt", 2442, null), new DAGFile("def.txt", 327879, null));
        Mockito.when(task.getOutputFiles()).thenReturn(files);
        skipEvent(100, WorkflowEvent.STORAGE_ALL_AFTER_TRANSFERS_COMPLETED, cloudsim);
        CloudSim.send(-1, storageManager.getId(), random.nextDouble(), WorkflowEvent.STORAGE_AFTER_TASK_COMPLETED, job);
        CloudSim.startSimulation();
        Mockito.verify(cloudsim).send(Matchers.anyInt(), Matchers.eq(100), Matchers.anyDouble(),
                Matchers.eq(WorkflowEvent.STORAGE_ALL_AFTER_TRANSFERS_COMPLETED), Matchers.any());
    }

    @Test(expected = UnknownWorkflowEventException.class)
    public void testUnknownMsg() {
        CloudSim.send(storageManager.getId(), storageManager.getId(), random.nextDouble(), 21434243, null);
        CloudSim.startSimulation();
    }

    @Test
    public void testTimeEstimationForEmptyTask() {
        Task t = new Task("xx", "xx", 222);
        Assert.assertEquals(0.0, storageManager.getTransferTimeEstimation(t), 0.0);
    }

    @Test
    public void testTimeEstimationForSimpleCase() {
        ImmutableList<DAGFile> files =
                ImmutableList.of(new DAGFile("abc.txt", 2442, null), new DAGFile("def.txt", 327879, null));
        Task t = new Task("xx", "xx", 222);
        t.addInputFiles(files);
        t.addOutputFiles(files);
        double time = storageManager.getTransferTimeEstimation(t);
        Assert.assertTrue(time >= 0.0); // just very simple assert, nothing more we can assume
    }

    @Test
    public void testStorageMangerUpdatesWriteStatistics() {
        ImmutableList<DAGFile> files =
                ImmutableList.of(new DAGFile("abc.txt", 333, null), new DAGFile("def.txt", 444, null));
        Mockito.when(task.getOutputFiles()).thenReturn(files);
        skipEvent(100, WorkflowEvent.STORAGE_ALL_AFTER_TRANSFERS_COMPLETED, cloudsim);
        CloudSim.send(-1, storageManager.getId(), random.nextDouble(), WorkflowEvent.STORAGE_AFTER_TASK_COMPLETED, job);
        CloudSim.startSimulation();
        Assert.assertEquals(777, storageManager.getStorageManagerStatistics().getTotalBytesToWrite());
        Assert.assertEquals(2, storageManager.getStorageManagerStatistics().getTotalFilesToWrite());

        Assert.assertEquals(0, storageManager.getStorageManagerStatistics().getTotalBytesToRead());
        Assert.assertEquals(0, storageManager.getStorageManagerStatistics().getActualBytesRead());
        Assert.assertEquals(0, storageManager.getStorageManagerStatistics().getTotalFilesToRead());
        Assert.assertEquals(0, storageManager.getStorageManagerStatistics().getActualFilesRead());
    }

    @Test
    public void testStorageMangerUpdatesReadStatistics() {
        ImmutableList<DAGFile> files =
                ImmutableList.of(new DAGFile("abc.txt", 222, null), new DAGFile("def.txt", 333, null));
        Mockito.when(task.getInputFiles()).thenReturn(files);
        skipEvent(100, WorkflowEvent.STORAGE_ALL_BEFORE_TRANSFERS_COMPLETED, cloudsim);
        CloudSim.send(-1, storageManager.getId(), random.nextDouble(), WorkflowEvent.STORAGE_BEFORE_TASK_START, job);
        CloudSim.startSimulation();
        Assert.assertEquals(555, storageManager.getStorageManagerStatistics().getTotalBytesToRead());
        Assert.assertEquals(555, storageManager.getStorageManagerStatistics().getActualBytesRead());
        Assert.assertEquals(2, storageManager.getStorageManagerStatistics().getTotalFilesToRead());
        Assert.assertEquals(2, storageManager.getStorageManagerStatistics().getActualFilesRead());

        Assert.assertEquals(0, storageManager.getStorageManagerStatistics().getTotalBytesToWrite());
        Assert.assertEquals(0, storageManager.getStorageManagerStatistics().getTotalFilesToWrite());
    }

    /** Skips event sent to by cloudsim obj. The rest is forwarded to the underlying CloudSim. */
    public static void skipEvent(int dst, int event, CloudSimWrapper cloudsim) {
        Mockito.doNothing().when(cloudsim)
                .send(Matchers.anyInt(), Matchers.eq(dst), Matchers.anyDouble(), Matchers.eq(event), Matchers.any());
    }
}
