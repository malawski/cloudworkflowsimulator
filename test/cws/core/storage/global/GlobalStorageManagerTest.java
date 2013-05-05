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
import cws.core.storage.StorageManagerTest;

/**
 * Tests {@link GlobalStorageManager}
 */
public class GlobalStorageManagerTest extends StorageManagerTest {
    private double readSpeed = 123;
    private double writeSpeed = 321;

    @Before
    public void setUp() {
        storageManager = new GlobalStorageManager(readSpeed, writeSpeed, cloudsim);
    }

    @Test
    public void testGlobalStorageReadTransferTime() {
        List<String> files = new ArrayList<String>();
        files.add("abc.txt");
        Mockito.when(task.getInputFiles()).thenReturn(files);
        skipEvent(100, WorkflowEvent.STORAGE_ALL_BEFORE_TRANSFERS_COMPLETED);
        CloudSim.send(-1, storageManager.getId(), random.nextDouble(), WorkflowEvent.STORAGE_BEFORE_TASK_START, job);
        double time = CloudSim.startSimulation();

        Mockito.verify(cloudsim).send(Matchers.anyInt(), Matchers.eq(100), Matchers.anyDouble(),
                Matchers.eq(WorkflowEvent.STORAGE_ALL_BEFORE_TRANSFERS_COMPLETED), Matchers.any());
        Assert.assertEquals(((long) 1000) / readSpeed, time, 0.74);
        // NOTE(bryk): 0.73 is CloudSim's error. Dunno why...
    }

    @Test
    public void testGlobalStorageWriteTransferTime() {
        List<String> files = new ArrayList<String>();
        files.add("abc.txt");
        Mockito.when(task.getOutputFiles()).thenReturn(files);
        skipEvent(100, WorkflowEvent.STORAGE_ALL_AFTER_TRANSFERS_COMPLETED);
        CloudSim.send(-1, storageManager.getId(), random.nextDouble(), WorkflowEvent.STORAGE_AFTER_TASK_COMPLETED, job);
        double time = CloudSim.startSimulation();

        Mockito.verify(cloudsim).send(Matchers.anyInt(), Matchers.eq(100), Matchers.anyDouble(),
                Matchers.eq(WorkflowEvent.STORAGE_ALL_AFTER_TRANSFERS_COMPLETED), Matchers.any());
        Assert.assertEquals(((long) 1000) / writeSpeed, time, 0.74);
        // NOTE(bryk): 0.73 is CloudSim's error. Dunno why...
    }
}
