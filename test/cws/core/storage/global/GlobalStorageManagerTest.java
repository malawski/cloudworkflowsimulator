package cws.core.storage.global;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import org.cloudbus.cloudsim.core.CloudSim;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import cws.core.Cloud;
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
        cacheManager = mock(VMCacheManager.class);
        doNothing().when(cacheManager).putFileToCache(Matchers.any(DAGFile.class), Matchers.any(Job.class));
        when(cacheManager.getFileFromCache(Matchers.any(DAGFile.class), Matchers.any(Job.class))).thenReturn(false);
        params = new GlobalStorageParams();
        params.setReadSpeed(123);
        params.setWriteSpeed(321);
        params.setLatency(20);
        params.setNumReplicas(1);
        params.setChunkTransferTime(1);
        storageManager = new GlobalStorageManager(params, cacheManager, cloudsim);
    }

    @Test
    public void testGlobalStorageReadTransferTime() {
        List<DAGFile> files = new ArrayList<DAGFile>();
        long sz = 2442;
        files.add(new DAGFile("abc.txt", sz));
        when(task.getInputFiles()).thenReturn(files);
        skipEvent(100, WorkflowEvent.STORAGE_ALL_BEFORE_TRANSFERS_COMPLETED, cloudsim);
        CloudSim.send(-1, storageManager.getId(), 0, WorkflowEvent.STORAGE_BEFORE_TASK_START, job);
        double time = CloudSim.startSimulation();

        verify(cloudsim).send(Matchers.anyInt(), Matchers.eq(100), Matchers.anyDouble(),
                Matchers.eq(WorkflowEvent.STORAGE_ALL_BEFORE_TRANSFERS_COMPLETED), Matchers.any());
        assertEquals(sz / params.getReadSpeed() + params.getLatency(), time, 0.01);
    }

    @Test
    public void testGlobalStorageWriteTransferTime() {
        List<DAGFile> files = new ArrayList<DAGFile>();
        long sz = 2442;
        files.add(new DAGFile("abc.txt", sz));
        when(task.getOutputFiles()).thenReturn(files);
        skipEvent(100, WorkflowEvent.STORAGE_ALL_AFTER_TRANSFERS_COMPLETED, cloudsim);
        CloudSim.send(-1, storageManager.getId(), 0, WorkflowEvent.STORAGE_AFTER_TASK_COMPLETED, job);
        double time = CloudSim.startSimulation();

        verify(cloudsim).send(Matchers.anyInt(), Matchers.eq(100), Matchers.anyDouble(),
                Matchers.eq(WorkflowEvent.STORAGE_ALL_AFTER_TRANSFERS_COMPLETED), Matchers.any());
        assertEquals(sz / params.getWriteSpeed() + params.getLatency(), time, 0.01);
    }

    @Test
    public void testGlobalStorageTwoFilesWriteTransferTime() {
        List<DAGFile> files = new ArrayList<DAGFile>();
        long sz = 2442;
        files.add(new DAGFile("abc.txt", sz));
        files.add(new DAGFile("abc2.txt", sz));
        when(task.getOutputFiles()).thenReturn(files);
        skipEvent(100, WorkflowEvent.STORAGE_ALL_AFTER_TRANSFERS_COMPLETED, cloudsim);
        CloudSim.send(-1, storageManager.getId(), 0, WorkflowEvent.STORAGE_AFTER_TASK_COMPLETED, job);
        double time = CloudSim.startSimulation();

        verify(cloudsim).send(Matchers.anyInt(), Matchers.eq(100), Matchers.anyDouble(),
                Matchers.eq(WorkflowEvent.STORAGE_ALL_AFTER_TRANSFERS_COMPLETED), Matchers.any());
        assertEquals(2 * (sz / params.getWriteSpeed() + params.getLatency()), time, 0.01);
    }

    @Test
    public void testGlobalStorageSimpleCongestionOneReplica() {
        params.setNumReplicas(1);
        params.setWriteSpeed(9713);
        long sz = 324324;
        double time = runTwoWrites(sz);
        assertEquals((sz * 2) / params.getWriteSpeed() + params.getLatency(), time, 0.01);
    }

    @Test
    public void testGlobalStorageSimpleCongestionTwoReplicas() {
        params.setNumReplicas(2);
        params.setWriteSpeed(9713);
        long sz = 324324;
        double time = runTwoWrites(sz);
        assertEquals(sz / params.getWriteSpeed() + params.getLatency(), time, 0.01);
    }

    @Test
    public void testGlobalStorageSimpleCongestionThousandReplicas() {
        params.setNumReplicas(1000);
        params.setWriteSpeed(9713);
        long sz = 324324;
        double time = runTwoWrites(sz);
        assertEquals(sz / params.getWriteSpeed() + params.getLatency(), time, 0.01);
    }

    /**
     * Runs two writes at the same time.
     * @return simulation time
     */
    private double runTwoWrites(long size) {
        Job job2 = Mockito.mock(Job.class);
        job2.setVM(vm);
        when(job2.getVM()).thenReturn(vm);
        Task task2 = Mockito.mock(Task.class);
        when(job2.getTask()).thenReturn(task2);
        List<DAGFile> files2 = new ArrayList<DAGFile>();
        files2.add(new DAGFile("abc2.txt", size));
        when(task2.getOutputFiles()).thenReturn(files2);

        List<DAGFile> files = new ArrayList<DAGFile>();
        files.add(new DAGFile("abc.txt", size));
        when(task.getOutputFiles()).thenReturn(files);
        skipEvent(100, WorkflowEvent.STORAGE_ALL_AFTER_TRANSFERS_COMPLETED, cloudsim);
        CloudSim.send(-1, storageManager.getId(), 0, WorkflowEvent.STORAGE_AFTER_TASK_COMPLETED, job);
        CloudSim.send(-1, storageManager.getId(), 0, WorkflowEvent.STORAGE_AFTER_TASK_COMPLETED, job2);
        double time = CloudSim.startSimulation();

        verify(cloudsim, Mockito.times(2)).send(Matchers.anyInt(), Matchers.eq(100), Matchers.anyDouble(),
                Matchers.eq(WorkflowEvent.STORAGE_ALL_AFTER_TRANSFERS_COMPLETED), Matchers.any());
        return time;
    }

    @Test
    public void testGlobalStorageComplexReadCongestion() {
        params.setNumReplicas(1000);
        long size = 2313244;
        List<DAGFile> files = new ArrayList<DAGFile>();
        files.add(new DAGFile("abc.txt", size));
        when(task.getInputFiles()).thenReturn(files);
        skipEvent(100, WorkflowEvent.STORAGE_ALL_BEFORE_TRANSFERS_COMPLETED, cloudsim);
        CloudSim.send(-1, storageManager.getId(), 0, WorkflowEvent.STORAGE_BEFORE_TASK_START, job);
        double time = CloudSim.startSimulation();

        verify(cloudsim).send(Matchers.anyInt(), Matchers.eq(100), Matchers.anyDouble(),
                Matchers.eq(WorkflowEvent.STORAGE_ALL_BEFORE_TRANSFERS_COMPLETED), Matchers.any());
        assertEquals(size / params.getReadSpeed() + params.getLatency(), time, 1.0);
    }

    @Test
    public void testGlobalStorageSimpleReadCongestion() {
        long size = 100000;
        int numSmaller = 10;
        Job job2 = Mockito.mock(Job.class);
        job2.setVM(vm);
        when(job2.getVM()).thenReturn(vm);
        Task task2 = Mockito.mock(Task.class);
        when(job2.getTask()).thenReturn(task2);
        List<DAGFile> files2 = new ArrayList<DAGFile>();
        for (int i = 0; i < numSmaller; i++) {
            files2.add(new DAGFile("abc.txt" + i, size / numSmaller));
        }
        when(task2.getInputFiles()).thenReturn(files2);

        List<DAGFile> files = new ArrayList<DAGFile>();
        files.add(new DAGFile("abc.txt", size));
        when(task.getInputFiles()).thenReturn(files);
        skipEvent(100, WorkflowEvent.STORAGE_ALL_BEFORE_TRANSFERS_COMPLETED, cloudsim);
        CloudSim.send(-1, storageManager.getId(), 0, WorkflowEvent.STORAGE_BEFORE_TASK_START, job);
        CloudSim.send(-1, storageManager.getId(), 0, WorkflowEvent.STORAGE_BEFORE_TASK_START, job2);
        double time = CloudSim.startSimulation();

        verify(cloudsim, Mockito.times(2)).send(Matchers.anyInt(), Matchers.eq(100), Matchers.anyDouble(),
                Matchers.eq(WorkflowEvent.STORAGE_ALL_BEFORE_TRANSFERS_COMPLETED), Matchers.any());
        assertEquals(time, 1744, 1.0);
    }

    @Test
    public void testTerminated() {
        long size = 1234567;
        double terminateTime = 444;
        Cloud cloud = new Cloud(cloudsim);

        List<DAGFile> files = new ArrayList<DAGFile>();
        files.add(new DAGFile("abc.txt", size));
        when(task.getOutputFiles()).thenReturn(files);
        skipEvent(100, WorkflowEvent.STORAGE_ALL_AFTER_TRANSFERS_COMPLETED, cloudsim);
        skipEvent(100, WorkflowEvent.VM_LAUNCH, cloudsim);
        skipEvent(100, WorkflowEvent.VM_LAUNCHED, cloudsim);
        skipEvent(100, WorkflowEvent.VM_TERMINATE, cloudsim);
        when(vm.isTerminated()).thenReturn(false);
        when(vm.getOwner()).thenReturn(100);
        Mockito.verifyNoMoreInteractions(vm);
        CloudSim.send(cloud.getId(), cloud.getId(), 0, WorkflowEvent.VM_LAUNCH, vm);
        CloudSim.send(-1, storageManager.getId(), 0, WorkflowEvent.STORAGE_AFTER_TASK_COMPLETED, job);
        CloudSim.send(cloud.getId(), cloud.getId(), terminateTime, WorkflowEvent.VM_TERMINATE, vm);
        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                when(vm.isTerminated()).thenReturn(true);
                return null;
            }
        })
                .when(cloudsim)
                .send(Matchers.anyInt(), Matchers.eq(100), Matchers.anyDouble(),
                        Matchers.eq(WorkflowEvent.VM_TERMINATED), Matchers.any());

        double time = CloudSim.startSimulation();

        assertEquals(terminateTime + params.getChunkTransferTime(), time, 0.01);
    }

    @Test
    public void testGlobalTimeInputEstimation() {
        List<DAGFile> files = new ArrayList<DAGFile>();
        long sz = 22222;
        files.add(new DAGFile("abc.txt", sz));
        Task t = new Task("xx", "xx", 222);
        t.addInputFiles(files);
        double time = storageManager.getTransferTimeEstimation(t);
        assertEquals(sz / params.getReadSpeed() + params.getLatency(), time, 0.00001);
    }

    @Test
    public void testGlobalTimeOutputEstimation() {
        List<DAGFile> files = new ArrayList<DAGFile>();
        long sz = 22222;
        files.add(new DAGFile("abc.txt", sz));
        Task t = new Task("xx", "xx", 222);
        t.addOutputFiles(files);
        double time = storageManager.getTransferTimeEstimation(t);
        assertEquals(sz / params.getWriteSpeed() + params.getLatency(), time, 0.00001);
    }
}
