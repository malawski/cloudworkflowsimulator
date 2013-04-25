package cws.core.scheduler;

import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyDouble;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.InOrder;

import cws.core.VM;
import cws.core.WorkflowEngine;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.dag.Task;
import cws.core.jobs.Job;

public class DAGDynamicSchedulerUnitTest {
	DAGDynamicScheduler scheduler;
	WorkflowEngine engine;
    CloudSimWrapper cloudsim;
	
	Queue<Job> jobs;
	Set<VM> freeVMs;	
	
	@Before
	public void setUp() throws Exception {
        cloudsim = mock(CloudSimWrapper.class);
		
        scheduler = new DAGDynamicScheduler(cloudsim);
		engine = mock(WorkflowEngine.class);
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
        Job job = createJobMock();
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
		jobs.add(createJobMock());
		
		Queue<Job> expected = jobs;
		
		scheduler.scheduleJobs(engine);		
		assertTrue(expected.equals(jobs));
	}
	
    @Test
	public void shouldAddTransferTaskForEachInputFile() {
        freeVMs.add(createVMMock());

        List<String> inputs = Arrays.asList("file1", "file2");
        List<String> outputs = Collections.emptyList();
        Job job = createJobMock(inputs, outputs);

        jobs.add(job);
        
        scheduler.scheduleJobs(engine);

        verify(cloudsim, times(3)).send(anyInt(), anyInt(), anyDouble(), anyInt(), any(Job.class));

	}

    @Test
    public void shouldAddTransferTaskForEachOutputFile() {
        freeVMs.add(createVMMock());

        List<String> inputs = Collections.emptyList();
        List<String> outputs = Arrays.asList("file1", "file2", "file3");
        Job job = createJobMock(inputs, outputs);

        jobs.add(job);

        scheduler.scheduleJobs(engine);

        verify(cloudsim, times(4)).send(anyInt(), anyInt(), anyDouble(), anyInt(), any(Job.class));

    }

    @Test
    public void shouldSendInputTransferBeforeAndOutputTransferAfter() {
        freeVMs.add(createVMMock());

        List<String> inputs = Arrays.asList("input-file");
        List<String> outputs = Arrays.asList("output-file");
        Job job = createJobMock(inputs, outputs);

        jobs.add(job);

        scheduler.scheduleJobs(engine);

        InOrder order = inOrder(cloudsim);
        order.verify(cloudsim).send(anyInt(), anyInt(), anyDouble(), anyInt(), argThat(new IsInputTransferJob()));
        order.verify(cloudsim).send(anyInt(), anyInt(), anyDouble(), anyInt(), eq(job));
        order.verify(cloudsim).send(anyInt(), anyInt(), anyDouble(), anyInt(), argThat(new IsOutputTransferJob()));

    }

    class IsInputTransferJob extends ArgumentMatcher<Job> {
        public boolean matches(Object job) {
            return ((Job) job).getTask().getId().startsWith("input-gs");
        }
    }

    class IsOutputTransferJob extends ArgumentMatcher<Job> {
        public boolean matches(Object job) {
            return ((Job) job).getTask().getId().startsWith("output-gs");
        }
    }

    private Job createJobMock(List<String> inputs, List<String> outputs) {
        Task task = mock(Task.class);
        Job job = new Job(20.0, 10.0);
		job.setTask(task);
		
        when(task.getInputFiles()).thenReturn(inputs);
        when(task.getOutputFiles()).thenReturn(outputs);

        when(task.getId()).thenReturn("");

		return job;
	}
    
    private Job createJobMock() {
        return createJobMock(new ArrayList<String>(), new ArrayList<String>());
    }
	
	private VM createVMMock() {
		return mock(VM.class);
	}
}
