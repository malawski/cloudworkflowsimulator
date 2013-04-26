package cws.core;

import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;

import cws.core.cloudsim.CWSSimEntity;
import cws.core.cloudsim.CWSSimEvent;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.dag.Task;
import cws.core.jobs.Job;
import cws.core.storage.VoidStorageManager;

public class VMTest {

    private CloudSimWrapper cloudsim;

    private class VMDriver extends CWSSimEntity {
        private VM vm;
        private Job[] jobs;

        public VMDriver(VM vm, CloudSimWrapper cloudsim) {
            super("VMDriver", cloudsim);
            this.vm = vm;
            getCloudsim().addEntity(this);
        }

        public void setJobs(Job[] jobs) {
            this.jobs = jobs;
        }

        @Override
        public void startEntity() {
            sendNow(vm.getId(), WorkflowEvent.VM_LAUNCH);

            // Submit all the jobs
            for (Job j : jobs) {
                j.setOwner(getId());
                getCloudsim().send(getId(), vm.getId(), 0.0, WorkflowEvent.JOB_SUBMIT, j);
            }
        }

        @Override
        public void processEvent(CWSSimEvent ev) {
            switch (ev.getTag()) {
            case WorkflowEvent.JOB_STARTED: {
                Job j = (Job) ev.getData();
                assertEquals(Job.State.RUNNING, j.getState());
                break;
            }
            case WorkflowEvent.JOB_FINISHED: {
                Job j = (Job) ev.getData();
                assertEquals(Job.State.TERMINATED, j.getState());
                break;
            }
            }
        }

        @Override
        public void shutdownEntity() {
        }
    }

    @Before
    public void setUp() {
        // TODO(_mequrel_): change to IoC in the future
        cloudsim = new CloudSimWrapper();
        cloudsim.init();
        // TODO(bryk): that's ugly, I know
        new VoidStorageManager(cloudsim);
    }

    @Test
    public void testSingleJob() {
        Job j = new Job(cloudsim);
        j.setTask(new Task("task_id", "transformation", 1000));

        VM vm = new VM(100, 1, 100, 0.40, cloudsim);

        VMDriver driver = new VMDriver(vm, cloudsim);
        driver.setJobs(new Job[] { j });

        cloudsim.startSimulation();

        assertEquals(0.0, j.getReleaseTime(), 0.0);
        assertEquals(0.0, j.getSubmitTime(), 0.0);
        assertEquals(0.0, j.getStartTime(), 0.0);
        assertEquals(10.0, j.getFinishTime(), 0.0);
    }

    @Test
    public void testTwoJobs() {
        Job j1 = new Job(cloudsim);
        j1.setTask(new Task("task_id", "transformation", 1000));
        Job j2 = new Job(cloudsim);
        j2.setTask(new Task("task_id2", "transformation", 1000));
        VM vm = new VM(100, 1, 100, 0.40, cloudsim);

        VMDriver driver = new VMDriver(vm, cloudsim);
        driver.setJobs(new Job[] { j1, j2 });

        cloudsim.startSimulation();

        assertEquals(0.0, j1.getReleaseTime(), 0.0);
        assertEquals(0.0, j1.getSubmitTime(), 0.0);
        assertEquals(0.0, j1.getStartTime(), 0.0);
        assertEquals(10.0, j1.getFinishTime(), 0.0);

        assertEquals(0.0, j2.getReleaseTime(), 0.0);
        assertEquals(0.0, j2.getSubmitTime(), 0.0);
        assertEquals(10.0, j2.getStartTime(), 0.0);
        assertEquals(20.0, j2.getFinishTime(), 0.0);
    }

    @Test
    public void testMultiCoreVM() {
        Job j1 = new Job(cloudsim);
        j1.setTask(new Task("task_id1", "transformation", 1000));

        Job j2 = new Job(cloudsim);
        j2.setTask(new Task("task_id2", "transformation", 1000));

        VM vm = new VM(100, 2, 100, 0.40, cloudsim);

        VMDriver driver = new VMDriver(vm, cloudsim);
        driver.setJobs(new Job[] { j1, j2 });

        cloudsim.startSimulation();

        assertEquals(0.0, j1.getReleaseTime(), 0.0);
        assertEquals(0.0, j1.getSubmitTime(), 0.0);
        assertEquals(0.0, j1.getStartTime(), 0.0);
        assertEquals(10.0, j1.getFinishTime(), 0.0);

        assertEquals(0.0, j2.getReleaseTime(), 0.0);
        assertEquals(0.0, j2.getSubmitTime(), 0.0);
        assertEquals(0.0, j2.getStartTime(), 0.0);
        assertEquals(10.0, j2.getFinishTime(), 0.0);
    }
}
