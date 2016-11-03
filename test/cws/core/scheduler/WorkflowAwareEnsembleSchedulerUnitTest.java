package cws.core.scheduler;

import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockingDetails;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import cws.core.pricing.PricingManager;
import org.cloudbus.cloudsim.core.CloudSim;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableList;

import cws.core.VM;
import cws.core.WorkflowEngine;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.core.VMType;
import cws.core.core.VMTypeBuilder;
import cws.core.dag.DAG;
import cws.core.dag.DAGFile;
import cws.core.dag.DAGJob;
import cws.core.dag.Task;
import cws.core.engine.Environment;
import cws.core.jobs.Job;
import org.mockito.internal.matchers.Any;

public class WorkflowAwareEnsembleSchedulerUnitTest {
    WorkflowAwareEnsembleScheduler scheduler;
    WorkflowEngine engine;
    CloudSimWrapper cloudsim;
    Environment environment;
    PricingManager pricingManager;

    List<Job> jobs;
    List<VM> freeVMs;

    @Before
    public void setUp() throws Exception {
        CloudSim.init(0, null, false);
        cloudsim = mock(CloudSimWrapper.class);
        when(cloudsim.clock()).thenReturn(1.0);

        environment = mock(Environment.class);

        when(environment.getVMTypePrice(any(VMType.class))).thenReturn(1.0);

        scheduler = new WorkflowAwareEnsembleScheduler(cloudsim, environment, new RuntimeWorkflowAdmissioner(cloudsim,
                new ComputationOnlyRuntimePredictioner(environment), environment, createVMType()));

        engine = mock(WorkflowEngine.class);
        when(engine.getDeadline()).thenReturn(10.0);
        when(engine.getBudget()).thenReturn(10.0);

        jobs = new LinkedList<Job>();
        freeVMs = new ArrayList<VM>();

        when(engine.getAndClearReleasedJobs()).thenReturn(jobs);
        when(engine.getFreeVMs()).thenReturn(freeVMs);

        pricingManager = mock(PricingManager.class);
    }

    @Test
    public void shouldDoNothingWithEmptyQueue() {
        freeVMs.add(createVMMock(createVMType()));
        // empty queues

        List<Job> expected = jobs;

        scheduler.scheduleJobs(engine);
        assertTrue(expected.equals(jobs));
    }

    @Test
    public void shouldScheduleFirstJobIfOneVMAvailable() {
        Job job = createSimpleJobMock();
        jobs.add(job);
        VM vm = createVMMock(createVMType());
        freeVMs.add(vm);

        when(environment.getComputationPredictedRuntimeForDAG(vm.getVmType(), job.getDAGJob().getDAG()))
                .thenReturn(10.0);
        when(environment.getPricingManager()).thenReturn(pricingManager);
        when(pricingManager.getRuntimeVMCost(any(VM.class))).thenReturn(0.0);
        when(pricingManager.getAlreadyPaidCost(any(VM.class))).thenReturn(0.0);

        scheduler.scheduleJobs(engine);

        Mockito.verify(vm, Mockito.times(1)).jobSubmit(job);
    }

    @Test
    public void shouldNotScheduleIfNoVMAvailable() {
        // empty VMs
        jobs.add(createSimpleJobMock());

        List<Job> expected = jobs;

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

    private Job createSimpleJobMock(ImmutableList<DAGFile> inputs, ImmutableList<DAGFile> outputs) {
        Task task = mock(Task.class);

        DAG dag = new DAG();
        dag.addTask(task);

        DAGJob dagjob = new DAGJob(dag, 0);

        Job job = new Job(dagjob, task, -1, cloudsim);

        when(task.getInputFiles()).thenReturn(inputs);
        when(task.getOutputFiles()).thenReturn(outputs);

        when(task.getId()).thenReturn("");

        return job;
    }

    private Job createSimpleJobMock() {
        return createSimpleJobMock(ImmutableList.<DAGFile> of(), ImmutableList.<DAGFile> of());
    }

    private VM createVMMock(VMType vmType) {
        VM vm = mock(VM.class);
        when(vm.getVmType()).thenReturn(vmType);
        return vm;
    }

    private VMType createVMType() {
        return VMTypeBuilder.newBuilder().mips(1).cores(1).price(1.0).build();
    }
}
