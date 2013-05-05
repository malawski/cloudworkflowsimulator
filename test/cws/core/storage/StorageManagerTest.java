package cws.core.storage;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.cloudbus.cloudsim.core.CloudSim;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

import cws.core.VM;
import cws.core.WorkflowEvent;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.dag.Task;
import cws.core.exception.UnknownWorkflowEventException;
import cws.core.jobs.Job;

/**
 * Tests {@link StorageManager}. An abstract class - should be subclassed and field {@link #storageManager} should be
 * initialized with correct subclass.
 */
public abstract class StorageManagerTest {
    protected Random random;
    protected StorageManager storageManager;
    protected CloudSimWrapper cloudsim;
    private Job job;
    private VM vm;
    private Task task;

    @Before
    public void setUpStorageManagerTest() {
        cloudsim = Mockito.mock(CloudSimWrapper.class);
        CloudSim.init(1, null, false);
        random = new Random(7);

        job = Mockito.mock(Job.class);
        vm = Mockito.mock(VM.class);
        Mockito.when(vm.getId()).thenReturn(100);
        job.setVM(vm);
        Mockito.when(job.getVM()).thenReturn(vm);
        task = Mockito.mock(Task.class);
        Mockito.when(job.getTask()).thenReturn(task);
    }

    @Test
    public void testEmptySimulation() {
        CloudSim.startSimulation();
    }

    @Test
    public void testBeforeTaskStartOnJobWithNoFiles() {
        Mockito.when(task.getInputFiles()).thenReturn(new ArrayList<String>());
        CloudSim.send(-1, storageManager.getId(), random.nextDouble(), WorkflowEvent.STORAGE_BEFORE_TASK_START, job);
        CloudSim.startSimulation();

        Mockito.verify(cloudsim).send(Matchers.anyInt(), Matchers.eq(100), Matchers.anyDouble(),
                Matchers.eq(WorkflowEvent.STORAGE_ALL_BEFORE_TRANSFERS_COMPLETED), Matchers.any());
    }

    @Test
    public void testAfterTaskCompletedOnJobWithNoFiles() {
        Mockito.when(task.getOutputFiles()).thenReturn(new ArrayList<String>());
        CloudSim.send(-1, storageManager.getId(), random.nextDouble(), WorkflowEvent.STORAGE_AFTER_TASK_COMPLETED, job);
        CloudSim.startSimulation();

        Mockito.verify(cloudsim).send(Matchers.anyInt(), Matchers.eq(100), Matchers.anyDouble(),
                Matchers.eq(WorkflowEvent.STORAGE_ALL_AFTER_TRANSFERS_COMPLETED), Matchers.any());
    }

    @Test
    public void testBeforeTaskStartOnJobWithTwoFiles() {
        List<String> files = new ArrayList<String>();
        files.add("abc.txt");
        files.add("def.txt");
        Mockito.when(task.getInputFiles()).thenReturn(files);
        CloudSim.send(-1, storageManager.getId(), random.nextDouble(), WorkflowEvent.STORAGE_BEFORE_TASK_START, job);
        CloudSim.startSimulation();

        Mockito.verify(cloudsim).send(Matchers.anyInt(), Matchers.eq(100), Matchers.anyDouble(),
                Matchers.eq(WorkflowEvent.STORAGE_ALL_BEFORE_TRANSFERS_COMPLETED), Matchers.any());
    }

    @Test
    public void testAfterTaskCompletedOnJobWithTwoFiles() {
        List<String> files = new ArrayList<String>();
        files.add("abc.txt");
        files.add("def.txt");
        Mockito.when(task.getOutputFiles()).thenReturn(files);
        CloudSim.send(-1, storageManager.getId(), random.nextDouble(), WorkflowEvent.STORAGE_AFTER_TASK_COMPLETED, job);
        CloudSim.startSimulation();

        Mockito.verify(cloudsim).send(Matchers.anyInt(), Matchers.eq(100), Matchers.anyDouble(),
                Matchers.eq(WorkflowEvent.STORAGE_ALL_AFTER_TRANSFERS_COMPLETED), Matchers.any());
    }

    @Test(expected = UnknownWorkflowEventException.class)
    public void testUnknownMsg() {
        CloudSim.send(storageManager.getId(), storageManager.getId(), random.nextDouble(), 21434243, null);
        CloudSim.startSimulation();
    }
}
