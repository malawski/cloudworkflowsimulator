package cws.core.dag;

import org.junit.Before;
import org.junit.Test;

import cws.core.FailureModel;
import cws.core.VM;
import cws.core.jobs.Job;
import cws.core.jobs.RuntimeDistribution;

/**
 * Generic tests for {@link Task} class. Tests contracts for this class. Should be subclassed to provide implementations
 * to test.
 */
public abstract class TaskTest {
    protected Task task;
    protected Job job;
    protected VM vm;
    protected RuntimeDistribution rd;
    protected FailureModel fm;

    public abstract void setUpTaskAndJob();

    /**
     * Sets up fields that probably will be used in in child classes.
     */
    @Before
    public void setUpTaskTest() {
        // TODO(bryk): Intentionally commented out. Will be implemented after closing #17

        // job = new Job();
        // vm = mock(VM.class);
        // rd = mock(RuntimeDistribution.class);
        // when(vm.getRuntimeDistribution()).thenReturn(rd);
        // when(rd.getActualRuntime(anyDouble())).thenReturn(200.0);
        // fm = mock(FailureModel.class);
        // when(vm.getFailureModel()).thenReturn(fm);
        // when(fm.failureOccurred()).thenReturn(false);// there's no failure by default
        // job.setVM(vm);
    }

    @Test
    public void taskShouldSendEventAfterFinish() {
        // TODO(bryk): Intentionally commented out. Will be implemented after closing #17
        // task.execute(job);
        // CloudSim.startSimulation();
        //
        // verify(vm).processEvent(Matchers.argThat(new ArgumentMatcher<SimEvent>() {
        // @Override
        // public boolean matches(Object argument) {
        // return argument instanceof SimEvent && ((SimEvent) argument).getType() == WorkflowEvent.JOB_FINISHED;
        // }
        // }));
    }
}
