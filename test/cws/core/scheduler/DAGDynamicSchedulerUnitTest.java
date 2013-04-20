package cws.core.scheduler;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import cws.core.Job;
import cws.core.VM;
import cws.core.WorkflowEngine;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.dag.Task;

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
		jobs.add(createJobMock());
		freeVMs.add(createVMMock());
		
		Queue<Job> expected = new LinkedList<Job>();
		
		scheduler.scheduleJobs(engine);
		assertTrue(expected.equals(jobs));
	}

	@Test
	public void shouldNotScheduleIfNoVMAvailable() {
		// empty VMs
		jobs.add(createJobMock());
		
		Queue<Job> expected = jobs;
		
		scheduler.scheduleJobs(engine);		
		assertTrue(expected.equals(jobs));
	}
	
	private Job createJobMock() {
		Task task = mock(Task.class);		
        Job job = new Job(20.0, 10.0);
		job.setTask(task);
		
		return job;
	}
	
	private VM createVMMock() {
		return mock(VM.class);
	}
}
