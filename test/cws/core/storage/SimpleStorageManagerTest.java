package cws.core.storage;

import java.util.Random;

import org.cloudbus.cloudsim.core.CloudSim;
import org.junit.Before;
import org.junit.Test;

import cws.core.Job;
import cws.core.SimEntityStub;
import cws.core.WorkflowEvent;
import cws.core.storage.global.GlobalStorageManager;

/**
 * Tests {@link StorageManager}.
 */
public class SimpleStorageManagerTest {
    private Random random;
    private StorageManager storageManager;

    @Before
    public void before() {
        CloudSim.init(1, null, false);
        random = new Random(7);
        storageManager = new GlobalStorageManager(0, 0);
    }

    @Test
    public void testEmptySimulation() {
        CloudSim.startSimulation();
    }

    @Test
    public void testOneFileSimulation() {
        CloudSim.addEntity(new SimEntityStub() {
            @Override
            public void startEntity() {
                send(storageManager.getId(), random.nextDouble(), WorkflowEvent.FILE_MANAGER_BEFORE_JOB_START, new Job(
                        100));
            }
        });
        CloudSim.startSimulation();
    }
}
