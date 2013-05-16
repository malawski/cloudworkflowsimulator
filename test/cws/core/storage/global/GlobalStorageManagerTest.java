package cws.core.storage.global;

import java.util.ArrayList;
import java.util.List;

import org.cloudbus.cloudsim.core.CloudSim;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

import cws.core.WorkflowEvent;
import cws.core.dag.DAGFile;
import cws.core.dag.Task;
import cws.core.storage.StorageManagerTest;

/**
 * Tests {@link GlobalStorageManager}
 */
public class GlobalStorageManagerTest extends StorageManagerTest {
    private GlobalStorageParams params;

    @Before
    public void setUp() {
        params = new GlobalStorageParams();
        params.setReadSpeed(123);
        params.setWriteSpeed(321);
        storageManager = new GlobalStorageManager(params, cloudsim);
    }

    @Test
    public void testGlobalStorageReadTransferTime() {
        List<DAGFile> files = new ArrayList<DAGFile>();
        double sz = 2442;
        files.add(new DAGFile("abc.txt", sz));
        Mockito.when(task.getInputFiles()).thenReturn(files);
        skipEvent(100, WorkflowEvent.STORAGE_ALL_BEFORE_TRANSFERS_COMPLETED);
        CloudSim.send(-1, storageManager.getId(), random.nextDouble(), WorkflowEvent.STORAGE_BEFORE_TASK_START, job);
        double time = CloudSim.startSimulation();

        Mockito.verify(cloudsim).send(Matchers.anyInt(), Matchers.eq(100), Matchers.anyDouble(),
                Matchers.eq(WorkflowEvent.STORAGE_ALL_BEFORE_TRANSFERS_COMPLETED), Matchers.any());
        Assert.assertEquals(sz / params.getReadSpeed(), time, 0.74);
        // NOTE(bryk): 0.73 is CloudSim's error. Dunno why...
    }

    @Test
    public void testGlobalStorageWriteTransferTime() {
        List<DAGFile> files = new ArrayList<DAGFile>();
        double sz = 2442;
        files.add(new DAGFile("abc.txt", sz));
        Mockito.when(task.getOutputFiles()).thenReturn(files);
        skipEvent(100, WorkflowEvent.STORAGE_ALL_AFTER_TRANSFERS_COMPLETED);
        CloudSim.send(-1, storageManager.getId(), random.nextDouble(), WorkflowEvent.STORAGE_AFTER_TASK_COMPLETED, job);
        double time = CloudSim.startSimulation();

        Mockito.verify(cloudsim).send(Matchers.anyInt(), Matchers.eq(100), Matchers.anyDouble(),
                Matchers.eq(WorkflowEvent.STORAGE_ALL_AFTER_TRANSFERS_COMPLETED), Matchers.any());
        Assert.assertEquals(sz / params.getWriteSpeed(), time, 0.74);
        // NOTE(bryk): 0.73 is CloudSim's error. Dunno why...
    }

    @Test
    public void testGlobalTimeInputEstimation() {
        List<DAGFile> files = new ArrayList<DAGFile>();
        double sz = 22222;
        files.add(new DAGFile("abc.txt", sz));
        Task t = new Task("xx", "xx", 222);
        t.setInputFiles(files);
        t.setOutputFiles(new ArrayList<DAGFile>());
        double time = storageManager.getTransferTimeEstimation(t);
        Assert.assertEquals(sz / params.getReadSpeed(), time, 0.00001);
    }

    @Test
    public void testGlobalTimeOutputEstimation() {
        List<DAGFile> files = new ArrayList<DAGFile>();
        double sz = 22222;
        files.add(new DAGFile("abc.txt", sz));
        Task t = new Task("xx", "xx", 222);
        t.setInputFiles(new ArrayList<DAGFile>());
        t.setOutputFiles(files);
        double time = storageManager.getTransferTimeEstimation(t);
        Assert.assertEquals(sz / params.getWriteSpeed(), time, 0.00001);
    }
}
