package cws.core.storage;

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
    private StorageManager storageManager;

    @Before
    public void before() {
        CloudSim.init(1, null, false);
        random = new Random(7);
        storageManager = new StorageManager();
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
                send(storageManager.getId(), random.nextDouble(), WorkflowEvent.NEW_FILE_TRANSFER, new FileTransfer());
            }
        });
        CloudSim.startSimulation();
    }
}
