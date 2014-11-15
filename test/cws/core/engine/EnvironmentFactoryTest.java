package cws.core.engine;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

import org.junit.Before;
import org.junit.Test;

import cws.core.cloudsim.CloudSimWrapper;
import cws.core.core.VMType;
import cws.core.core.VMTypeBuilder;
import cws.core.simulation.StorageCacheType;
import cws.core.simulation.StorageSimulationParams;
import cws.core.simulation.StorageType;

/**
 * Tests for {@link EnvironmentFactory} class.
 */
public class EnvironmentFactoryTest {
    private StorageSimulationParams storageParams;
    private CloudSimWrapper cloudsim;
    private VMType vmType;

    @Before
    public void before() {
        cloudsim = new CloudSimWrapper();
        cloudsim.init();
        storageParams = new StorageSimulationParams(StorageType.VOID, null, StorageCacheType.FIFO);
        vmType = VMTypeBuilder.newBuilder().mips(1).cores(1).price(1.0).build();
    }

    @Test
    public void testCreateStorageAwareEnvironment() {
        Environment environment = EnvironmentFactory.createEnvironment(cloudsim, storageParams, vmType, true);
        assertTrue(environment.isStorageAware());
    }

    @Test
    public void testCreateStorageUnawareEnvironment() {
        Environment environment = EnvironmentFactory.createEnvironment(cloudsim, storageParams, vmType, false);
        assertFalse(environment.isStorageAware());
    }
}
