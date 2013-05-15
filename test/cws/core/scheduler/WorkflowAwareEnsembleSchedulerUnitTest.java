package cws.core.scheduler;

import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyDouble;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;

import cws.core.storage.StorageManager;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatcher;

import cws.core.VM;
import cws.core.WorkflowEngine;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.dag.DAG;
import cws.core.dag.DAGFile;
import cws.core.dag.DAGJob;
import cws.core.dag.Task;
import cws.core.jobs.Job;

public class WorkflowAwareEnsembleSchedulerUnitTest {
    WorkflowAwareEnsembleScheduler scheduler;
    WorkflowEngine engine;
    CloudSimWrapper cloudsim;
    StorageManager storageManager;

    Queue<Job> jobs;
    Set<VM> freeVMs;

    @Before
    public void setUp() throws Exception {
        cloudsim = mock(CloudSimWrapper.class);
        when(cloudsim.clock()).thenReturn(1.0);

        storageManager = mock(StorageManager.class);

        scheduler = new WorkflowAwareEnsembleScheduler(cloudsim, storageManager);

        engine = mock(WorkflowEngine.class);
        when(engine.getDeadline()).thenReturn(10.0);
        when(engine.getBudget()).thenReturn(10.0);

        scheduler.setWorkflowEngine(engine);

        jobs = new LinkedList<Job>();
        freeVMs = new HashSet<VM>();

        when(engine.getQueuedJobs()).thenReturn(jobs);
        when(engine.getFreeVMs()).thenReturn(freeVMs);
    }

    @Test
    public void shouldDoNothingWithEmptyQueue() {
        freeVMs.add(createVMMock());
        // empty queues

        Queue<Job> expected = jobs;

        scheduler.scheduleJobs(engine);
        assertTrue(expected.equals(jobs));
    }

    @Test
    public void shouldScheduleFirstJobIfOneVMAvailable() {
        Job job = createSimpleJobMock();
        jobs.add(job);
        freeVMs.add(createVMMock());

        Queue<Job> expected = new LinkedList<Job>();

        scheduler.scheduleJobs(engine);

        assertTrue(expected.equals(jobs));
        verify(cloudsim, times(1)).send(anyInt(), anyInt(), anyDouble(), anyInt(), eq(job));
    }

    @Test
    public void shouldNotScheduleIfNoVMAvailable() {
        // empty VMs
        jobs.add(createSimpleJobMock());

        Queue<Job> expected = jobs;

        scheduler.scheduleJobs(engine);
        assertTrue(expected.equals(jobs));
    }

    class IsInputTransferJob extends ArgumentMatcher<Job> {
        @Override
        public boolean matches(Object job) {
            return ((Job) job).getTask().getId().startsWith("input-gs");
        }
    }

    class IsOutputTransferJob extends ArgumentMatcher<Job> {
        @Override
        public boolean matches(Object job) {
            return ((Job) job).getTask().getId().startsWith("output-gs");
        }
    }

    private Job createSimpleJobMock(List<DAGFile> inputs, List<DAGFile> outputs) {
        Task task = mock(Task.class);

        DAG dag = new DAG();
        dag.addTask(task);

        DAGJob dagjob = new DAGJob(dag, 0);

        Job job = new Job(cloudsim);
        job.setTask(task);
        job.setDAGJob(dagjob);

        when(task.getInputFiles()).thenReturn(inputs);
        when(task.getOutputFiles()).thenReturn(outputs);

        when(task.getId()).thenReturn("");

        return job;
    }

    private Job createSimpleJobMock() {
        return createSimpleJobMock(new ArrayList<DAGFile>(), new ArrayList<DAGFile>());
    }

    private VM createVMMock() {
        return mock(VM.class);
    }
}
