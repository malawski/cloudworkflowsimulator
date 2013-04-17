package cws.core.storage.global;

import java.util.Random;

import org.cloudbus.cloudsim.core.CloudSim;
import org.junit.Before;
import org.junit.Test;

import cws.core.SimEntityStub;
import cws.core.WorkflowEvent;

/**
 * Tests {@link StorageManager}.
 */
public class SimpleStorageManagerTest {
    private Random random;
    private GlobalStorageManager storageManager;

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
                send(storageManager.getId(), random.nextDouble(), WorkflowEvent.GLOBAL_STORAGE_START_READ, null);
            }
        });
        CloudSim.startSimulation();
    }
}
