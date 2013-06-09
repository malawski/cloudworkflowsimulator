package cws.core.storage.global;

import java.util.ArrayList;

import org.cloudbus.cloudsim.core.CloudSim;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

import cws.core.VM;
import cws.core.WorkflowEvent;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.dag.DAGFile;
import cws.core.dag.Task;
import cws.core.jobs.Job;
import cws.core.storage.StorageManagerTest;
import cws.core.storage.cache.FIFOCacheManager;
import cws.core.storage.cache.VMCacheManager;

/**
 * Tests {@link GlobalStorageManager} with {@link FIFOCacheManager} as cache manager.
 */
public class GlobalStorageManagerCacheTest {
    private CloudSimWrapper cloudsim;
    private Job job;
    private VM vm;
    private Task task;
    private VMCacheManager cacheManager;
    private GlobalStorageParams params;
    private GlobalStorageManager storageManager;
    private ArrayList<DAGFile> files;
    private int sz;
    private DAGFile df;

    @Before
    public void setUp() {
        cloudsim = Mockito.spy(new CloudSimWrapper());
        cloudsim.init();
        job = Mockito.mock(Job.class);
        vm = Mockito.mock(VM.class);
        Mockito.when(vm.getId()).thenReturn(100);
        job.setVM(vm);
        Mockito.when(job.getVM()).thenReturn(vm);
        task = Mockito.mock(Task.class);
        Mockito.when(job.getTask()).thenReturn(task);
        cacheManager = Mockito.mock(VMCacheManager.class);
        params = new GlobalStorageParams();
        params.setReadSpeed(123);
        params.setWriteSpeed(321);

        storageManager = new GlobalStorageManager(params, cacheManager, cloudsim);
        files = new ArrayList<DAGFile>();
        sz = 2442;
        df = new DAGFile("abc.txt", sz);
        files.add(df);
    }

    @Test
    public void testStorageCachesInputFiles() {
        Mockito.doNothing().when(cacheManager).putFileToCache(Matchers.eq(df), Matchers.eq(job));
        Mockito.when(cacheManager.getFileFromCache(Matchers.eq(df), Matchers.eq(job))).thenReturn(false);

        Mockito.when(task.getInputFiles()).thenReturn(files);
        StorageManagerTest.skipEvent(100, WorkflowEvent.STORAGE_ALL_BEFORE_TRANSFERS_COMPLETED, cloudsim);
        CloudSim.send(-1, storageManager.getId(), 0, WorkflowEvent.STORAGE_BEFORE_TASK_START, job);
        CloudSim.startSimulation();

        Mockito.verify(cacheManager).getFileFromCache(df, job); // tried to get ...
        Mockito.verify(cacheManager).putFileToCache(df, job); // and then put
        Mockito.verifyNoMoreInteractions(cacheManager);
        Mockito.verify(cloudsim).send(Matchers.anyInt(), Matchers.eq(100), Matchers.anyDouble(),
                Matchers.eq(WorkflowEvent.STORAGE_ALL_BEFORE_TRANSFERS_COMPLETED), Matchers.any());
    }

    @Test
    public void testStorageGetsInputFilesFromCache() {
        Mockito.doNothing().when(cacheManager).putFileToCache(Matchers.eq(df), Matchers.eq(job));
        Mockito.when(cacheManager.getFileFromCache(Matchers.eq(df), Matchers.eq(job))).thenReturn(true);

        Mockito.when(task.getInputFiles()).thenReturn(files);
        StorageManagerTest.skipEvent(100, WorkflowEvent.STORAGE_ALL_BEFORE_TRANSFERS_COMPLETED, cloudsim);
        CloudSim.send(-1, storageManager.getId(), 0, WorkflowEvent.STORAGE_BEFORE_TASK_START, job);
        Assert.assertEquals(0.0, CloudSim.startSimulation(), 0.0001);

        Mockito.verify(cacheManager).getFileFromCache(df, job);
        Mockito.verifyNoMoreInteractions(cacheManager);
        Mockito.verify(cloudsim).send(Matchers.anyInt(), Matchers.eq(100), Matchers.anyDouble(),
                Matchers.eq(WorkflowEvent.STORAGE_ALL_BEFORE_TRANSFERS_COMPLETED), Matchers.any());
    }

    @Test
    public void testGlobalStorageMangerUpdatesReadStatistics() {
        Mockito.doNothing().when(cacheManager).putFileToCache(Matchers.eq(df), Matchers.eq(job));
        Mockito.when(cacheManager.getFileFromCache(Matchers.eq(df), Matchers.eq(job))).thenReturn(true);

        Mockito.when(task.getInputFiles()).thenReturn(files);
        StorageManagerTest.skipEvent(100, WorkflowEvent.STORAGE_ALL_BEFORE_TRANSFERS_COMPLETED, cloudsim);
        CloudSim.send(-1, storageManager.getId(), 0, WorkflowEvent.STORAGE_BEFORE_TASK_START, job);
        CloudSim.startSimulation();

        Assert.assertEquals(2442, storageManager.getStorageManagerStatistics().getTotalBytesToRead());
        Assert.assertEquals(0, storageManager.getStorageManagerStatistics().getActualBytesRead());
    }

    @Test
    public void testStorageCachesOutputFiles() {
        Mockito.doNothing().when(cacheManager).putFileToCache(Matchers.eq(df), Matchers.eq(job));

        Mockito.when(task.getOutputFiles()).thenReturn(files);
        StorageManagerTest.skipEvent(100, WorkflowEvent.STORAGE_ALL_AFTER_TRANSFERS_COMPLETED, cloudsim);
        CloudSim.send(-1, storageManager.getId(), 0, WorkflowEvent.STORAGE_AFTER_TASK_COMPLETED, job);
        CloudSim.startSimulation();

        Mockito.verify(cacheManager).putFileToCache(df, job); // only saves to cache
        Mockito.verifyNoMoreInteractions(cacheManager);
        Mockito.verify(cloudsim).send(Matchers.anyInt(), Matchers.eq(100), Matchers.anyDouble(),
                Matchers.eq(WorkflowEvent.STORAGE_ALL_AFTER_TRANSFERS_COMPLETED), Matchers.any());
    }
}
