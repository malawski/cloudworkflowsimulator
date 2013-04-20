package cws.core.storage.global;

import java.util.Random;

import org.cloudbus.cloudsim.core.CloudSim;
import org.junit.Before;
import org.junit.Test;

import cws.core.SimEntityStub;
import cws.core.WorkflowEvent;
import cws.core.cloudsim.CloudSimWrapper;

/**
 * Tests {@link StorageManager}.
 */
public class SimpleStorageManagerTest {
    private Random random;
    private GlobalStorageManager storageManager;

    private CloudSimWrapper cloudsim;

    @Before
    public void before() {
        CloudSim.init(1, null, false);
        random = new Random(7);

        // TODO(_mequrel_): change to IoC in the future or to mock
        cloudsim = new CloudSimWrapper();

        storageManager = new GlobalStorageManager(0, 0, cloudsim);
    }

    @Test
    public void testEmptySimulation() {
        CloudSim.startSimulation();
    }

    @Test
    public void testOneFileSimulation() {
        CloudSim.addEntity(new SimEntityStub(cloudsim) {
            @Override
            public void startEntity() {
                send(storageManager.getId(), random.nextDouble(), WorkflowEvent.GLOBAL_STORAGE_START_READ, null);
            }
        });
        CloudSim.startSimulation();
    }
}
