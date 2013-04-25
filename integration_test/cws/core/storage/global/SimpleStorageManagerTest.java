package cws.core.storage.global;

import java.util.Random;

import org.junit.Before;
import org.junit.Test;

import cws.core.WorkflowEvent;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.stub.SimEntityStub;

/**
 * Tests {@link StorageManager}.
 */
public class SimpleStorageManagerTest {
    private Random random;
    private GlobalStorageManager storageManager;

    private CloudSimWrapper cloudsim;

    @Before
    public void before() {
        // TODO(_mequrel_): change to IoC in the future or to mock
        cloudsim = new CloudSimWrapper();
        cloudsim.init(1, null, false);

        random = new Random(7);

        storageManager = new GlobalStorageManager(0, 0, cloudsim);
    }

    @Test
    public void testEmptySimulation() {
        cloudsim.startSimulation();
    }

    @Test
    public void testOneFileSimulation() {
        cloudsim.addEntity(new SimEntityStub(cloudsim) {
            @Override
            public void startEntity() {
                getCloudsim().send(getId(), storageManager.getId(), random.nextDouble(),
                        WorkflowEvent.GLOBAL_STORAGE_START_READ, null);
            }
        });
        cloudsim.startSimulation();
    }
}
