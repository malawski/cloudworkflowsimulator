package cws.core.engine;

import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import cws.core.core.VMType;
import cws.core.core.VMTypeBuilder;
import cws.core.dag.Task;
import cws.core.storage.StorageManager;

/**
 * Tests for {@link StorageAwarePredictionStrategy} class.
 */
public class StorageAwarePredictionStrategyTest {
    private StorageAwarePredictionStrategy strategy;
    private StorageManager storageManager;
    private VMType vmType = VMTypeBuilder.newBuilder().mips(1).cores(1).price(1.0).build();

    @Before
    public void setUp() throws Exception {
        storageManager = Mockito.mock(StorageManager.class);
        strategy = new StorageAwarePredictionStrategy();
    }

    @Test
    public void testGetPredictedRuntimeTask() {
        Task task = new Task("id", "trans", 123);
        Mockito.when(storageManager.getTransferTimeEstimation(task)).thenReturn(777.0);
        assertEquals(900, strategy.getPredictedRuntime(task, vmType, storageManager), 0.00001);
    }
}
