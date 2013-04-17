package cws.core.scheduler;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import cws.core.Job;
import cws.core.VM;
import cws.core.WorkflowEngine;
import cws.core.dag.Task;

public class DAGDynamicSchedulerUnitTest {
	DAGDynamicScheduler scheduler;
	WorkflowEngine engine;
	
	Queue<Job> jobs;
	Set<VM> freeVMs;
	Set<VM> busyVMs;
	
	
	@Before
	public void setUp() throws Exception {
		scheduler = new DAGDynamicScheduler();
		engine = mock(WorkflowEngine.class);
		scheduler.setWorkflowEngine(engine);
		
		jobs = new LinkedList<Job>();
		freeVMs = new HashSet<VM>();
		busyVMs = new HashSet<VM>();
		
		when(engine.getQueuedJobs()).thenReturn(jobs);
		when(engine.getFreeVMs()).thenReturn(freeVMs);
		when(engine.getBusyVMs()).thenReturn(busyVMs);
	}

	@Test
	public void shouldDoNothingWithEmptyQueue() {
		freeVMs.add(mock(VM.class));
		// empty queues
		
		Queue<Job> expected = jobs;
		
		scheduler.scheduleJobs(engine);
		assertTrue(expected.equals(jobs));
	}
	
	@Test
	public void shouldScheduleFirstJobIfOneVMAvailable() {
		jobs.add(createJobMock());
		freeVMs.add(mock(VM.class));
		
		Queue<Job> expected = new LinkedList<Job>();
		
		scheduler.scheduleJobs(engine);
		assertTrue(expected.equals(jobs));
	}

	private Job createJobMock() {
		Task task = mock(Task.class);		
		Job job = new Job(20.0);
		job.setTask(task);
		
		return job;
	}

	@Test
	public void shouldNotScheduleIfNoVMAvailable() {
		// empty VMs
		jobs.add(createJobMock());
		
		Queue<Job> expected = jobs;
		
		scheduler.scheduleJobs(engine);		
		assertTrue(expected.equals(jobs));
	}
}
