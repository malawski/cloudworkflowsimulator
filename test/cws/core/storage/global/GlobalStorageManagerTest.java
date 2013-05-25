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
import cws.core.jobs.Job;
import cws.core.storage.StorageManagerTest;
import cws.core.storage.cache.VMCacheManager;

/**
 * Tests {@link GlobalStorageManager} with "always empty" mocked cache.
 */
public class GlobalStorageManagerTest extends StorageManagerTest {
    protected GlobalStorageParams params;
    protected VMCacheManager cacheManager;

    @Before
    public void setUpGlobalStorageManagerTest() {
        cacheManager = Mockito.mock(VMCacheManager.class);
        Mockito.doNothing().when(cacheManager).putFileToCache(Matchers.any(DAGFile.class), Matchers.any(Job.class));
        Mockito.when(cacheManager.getFileFromCache(Matchers.any(DAGFile.class), Matchers.any(Job.class))).thenReturn(
                false);
        params = new GlobalStorageParams();
        params.setReadSpeed(123);
        params.setWriteSpeed(321);
        storageManager = new GlobalStorageManager(params, cacheManager, cloudsim);
    }

    @Test
    public void testGlobalStorageReadTransferTime() {
        List<DAGFile> files = new ArrayList<DAGFile>();
        long sz = 2442;
        files.add(new DAGFile("abc.txt", sz));
        Mockito.when(task.getInputFiles()).thenReturn(files);
        skipEvent(100, WorkflowEvent.STORAGE_ALL_BEFORE_TRANSFERS_COMPLETED, cloudsim);
        CloudSim.send(-1, storageManager.getId(), 0, WorkflowEvent.STORAGE_BEFORE_TASK_START, job);
        double time = CloudSim.startSimulation();

        Mockito.verify(cloudsim).send(Matchers.anyInt(), Matchers.eq(100), Matchers.anyDouble(),
                Matchers.eq(WorkflowEvent.STORAGE_ALL_BEFORE_TRANSFERS_COMPLETED), Matchers.any());
        Assert.assertEquals(sz / params.getReadSpeed() + params.getLatency(), time, 0.01);
    }

    @Test
    public void testGlobalStorageWriteTransferTime() {
        List<DAGFile> files = new ArrayList<DAGFile>();
        long sz = 2442;
        files.add(new DAGFile("abc.txt", sz));
        Mockito.when(task.getOutputFiles()).thenReturn(files);
        skipEvent(100, WorkflowEvent.STORAGE_ALL_AFTER_TRANSFERS_COMPLETED, cloudsim);
        CloudSim.send(-1, storageManager.getId(), 0, WorkflowEvent.STORAGE_AFTER_TASK_COMPLETED, job);
        double time = CloudSim.startSimulation();

        Mockito.verify(cloudsim).send(Matchers.anyInt(), Matchers.eq(100), Matchers.anyDouble(),
                Matchers.eq(WorkflowEvent.STORAGE_ALL_AFTER_TRANSFERS_COMPLETED), Matchers.any());
        Assert.assertEquals(sz / params.getWriteSpeed() + params.getLatency(), time, 0.01);
    }

    @Test
    public void testGlobalTimeInputEstimation() {
        List<DAGFile> files = new ArrayList<DAGFile>();
        long sz = 22222;
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
        long sz = 22222;
        files.add(new DAGFile("abc.txt", sz));
        Task t = new Task("xx", "xx", 222);
        t.setInputFiles(new ArrayList<DAGFile>());
        t.setOutputFiles(files);
        double time = storageManager.getTransferTimeEstimation(t);
        Assert.assertEquals(sz / params.getWriteSpeed(), time, 0.00001);
    }
}
