package cws.core.storage.global;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
    private ImmutableList<DAGFile> files;
    private int sz;
    private DAGFile df;

    @Before
    public void setUp() {
        cloudsim = Mockito.spy(new CloudSimWrapper());
        cloudsim.init();
        job = Mockito.mock(Job.class);
        vm = Mockito.mock(VM.class);
        when(vm.getId()).thenReturn(100);
        job.setVM(vm);
        when(job.getVM()).thenReturn(vm);
        task = Mockito.mock(Task.class);
        when(job.getTask()).thenReturn(task);
        cacheManager = Mockito.mock(VMCacheManager.class);
        params = new GlobalStorageParams();
        params.setReadSpeed(123);
        params.setWriteSpeed(321);

        storageManager = new GlobalStorageManager(params, cacheManager, cloudsim);
        sz = 2442;
        df = new DAGFile("abc.txt", sz, null);
        files = ImmutableList.of(df);
    }

    @Test
    public void testStorageCachesInputFiles() {
        Mockito.doNothing().when(cacheManager).putFileToCache(eq(df), eq(vm));
        when(cacheManager.getFileFromCache(eq(df), eq(vm))).thenReturn(false);

        when(task.getInputFiles()).thenReturn(files);
        StorageManagerTest.skipEvent(100, WorkflowEvent.STORAGE_ALL_BEFORE_TRANSFERS_COMPLETED, cloudsim);
        CloudSim.send(-1, storageManager.getId(), 0, WorkflowEvent.STORAGE_BEFORE_TASK_START, job);
        CloudSim.startSimulation();

        verify(cacheManager, Mockito.atLeastOnce()).getFileFromCache(df, job.getVM()); // tried to get ...
        verify(cacheManager).putFileToCache(df, job.getVM()); // and then put
        Mockito.verifyNoMoreInteractions(cacheManager);
        verify(cloudsim).send(Matchers.anyInt(), eq(100), Matchers.anyDouble(),
                eq(WorkflowEvent.STORAGE_ALL_BEFORE_TRANSFERS_COMPLETED), Matchers.any());
    }

    @Test
    public void testStorageGetsInputFilesFromCache() {
        Mockito.doNothing().when(cacheManager).putFileToCache(eq(df), eq(vm));
        when(cacheManager.getFileFromCache(eq(df), eq(vm))).thenReturn(true);

        when(task.getInputFiles()).thenReturn(files);
        StorageManagerTest.skipEvent(100, WorkflowEvent.STORAGE_ALL_BEFORE_TRANSFERS_COMPLETED, cloudsim);
        CloudSim.send(-1, storageManager.getId(), 0, WorkflowEvent.STORAGE_BEFORE_TASK_START, job);
        assertEquals(0.01, CloudSim.startSimulation(), 0.01); // Cache latency.

        verify(cacheManager, Mockito.atLeastOnce()).getFileFromCache(df, job.getVM());
        verify(cacheManager).putFileToCache(df, job.getVM());
        Mockito.verifyNoMoreInteractions(cacheManager);
        verify(cloudsim).send(Matchers.anyInt(), eq(100), Matchers.anyDouble(),
                eq(WorkflowEvent.STORAGE_ALL_BEFORE_TRANSFERS_COMPLETED), Matchers.any());
    }

    @Test
    public void testGlobalStorageMangerUpdatesReadStatistics() {
        Mockito.doNothing().when(cacheManager).putFileToCache(eq(df), eq(vm));
        when(cacheManager.getFileFromCache(eq(df), eq(vm))).thenReturn(true);

        when(task.getInputFiles()).thenReturn(files);
        StorageManagerTest.skipEvent(100, WorkflowEvent.STORAGE_ALL_BEFORE_TRANSFERS_COMPLETED, cloudsim);
        CloudSim.send(-1, storageManager.getId(), 0, WorkflowEvent.STORAGE_BEFORE_TASK_START, job);
        CloudSim.startSimulation();

        assertEquals(2442, storageManager.getStorageManagerStatistics().getTotalBytesToRead());
        assertEquals(2442, storageManager.getStorageManagerStatistics().getBytesReadFromCache());
    }

    @Test
    public void testStorageCachesOutputFiles() {
        Mockito.doNothing().when(cacheManager).putFileToCache(eq(df), eq(vm));

        when(task.getOutputFiles()).thenReturn(files);
        StorageManagerTest.skipEvent(100, WorkflowEvent.STORAGE_ALL_AFTER_TRANSFERS_COMPLETED, cloudsim);
        CloudSim.send(-1, storageManager.getId(), 0, WorkflowEvent.STORAGE_AFTER_TASK_COMPLETED, job);
        CloudSim.startSimulation();

        verify(cacheManager).putFileToCache(df, job.getVM()); // only saves to cache
        Mockito.verifyNoMoreInteractions(cacheManager);
        verify(cloudsim).send(Matchers.anyInt(), eq(100), Matchers.anyDouble(),
                eq(WorkflowEvent.STORAGE_ALL_AFTER_TRANSFERS_COMPLETED), Matchers.any());
    }
}
