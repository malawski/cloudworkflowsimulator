package cws.core.engine;

import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import cws.core.core.VMType;
import cws.core.core.VMTypeBuilder;
import cws.core.dag.DAG;
import cws.core.dag.Task;
import cws.core.storage.StorageManager;

/**
 * Tests for {@link StorageUnawareEnvironment} class.
 */
public class StorageUnawareEnvironmentTest {
    private StorageUnawareEnvironment environment;
    private StorageManager storageManager;
    private VMType vmType = VMTypeBuilder.newBuilder().mips(1).cores(1).price(1.0).build();

    @Before
    public void setUp() throws Exception {
        storageManager = Mockito.mock(StorageManager.class);
        environment = new StorageUnawareEnvironment(vmType, storageManager);
    }

    @Test
    public void testGetPredictedRuntimeTask() {
        Task task = new Task("id", "trans", 123);
        assertEquals(123, environment.getPredictedRuntime(task), 0.00001);
    }

    @Test
    public void testGetPredictedRuntimeDAG() {
        Task task1 = new Task("id", "trans", 123);
        Task task2 = new Task("id2", "trans", 321);
        DAG dag = new DAG();
        dag.addTask(task1);
        dag.addTask(task2);
        assertEquals(444, environment.getPredictedRuntime(dag), 0.00001);
    }
}
