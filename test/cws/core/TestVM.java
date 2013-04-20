package cws.core;

import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;

import cws.core.cloudsim.CWSSimEntity;
import cws.core.cloudsim.CWSSimEvent;
import cws.core.cloudsim.CloudSimWrapper;

public class TestVM {

    private CloudSimWrapper cloudsim;

    private class VMDriver extends CWSSimEntity implements WorkflowEvent {
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
            sendNow(vm.getId(), VM_LAUNCH);

            // Submit all the jobs
            for (Job j : jobs) {
                j.setOwner(getId());
                send(vm.getId(), 0.0, JOB_SUBMIT, j);
            }
        }

        @Override
        public void processEvent(CWSSimEvent ev) {
            switch (ev.getTag()) {
            case JOB_STARTED: {
                Job j = (Job) ev.getData();
                assertEquals(Job.State.RUNNING, j.getState());
                break;
            }
            case JOB_FINISHED: {
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
        cloudsim.init(1, null, false);
    }

    @Test
    public void testSingleJob() {
        Job j = new Job(1000, cloudsim.clock());

        VM vm = new VM(100, 1, 100, 0.40, cloudsim);

        VMDriver driver = new VMDriver(vm, cloudsim);
        driver.setJobs(new Job[] { j });

        cloudsim.startSimulation();

        assertEquals(j.getReleaseTime(), 0.0, 0.0);
        assertEquals(j.getSubmitTime(), 0.0, 0.0);
        assertEquals(j.getStartTime(), 0.0, 0.0);
        assertEquals(j.getFinishTime(), 10.0, 0.0);
    }

    @Test
    public void testTwoJobs() {
        Job j1 = new Job(1000, cloudsim.clock());
        Job j2 = new Job(1000, cloudsim.clock());

        VM vm = new VM(100, 1, 100, 0.40, cloudsim);

        VMDriver driver = new VMDriver(vm, cloudsim);
        driver.setJobs(new Job[] { j1, j2 });

        cloudsim.startSimulation();

        assertEquals(j1.getReleaseTime(), 0.0, 0.0);
        assertEquals(j1.getSubmitTime(), 0.0, 0.0);
        assertEquals(j1.getStartTime(), 0.0, 0.0);
        assertEquals(j1.getFinishTime(), 10.0, 0.0);

        assertEquals(j2.getReleaseTime(), 0.0, 0.0);
        assertEquals(j2.getSubmitTime(), 0.0, 0.0);
        assertEquals(j2.getStartTime(), 10.0, 0.0);
        assertEquals(j2.getFinishTime(), 20.0, 0.0);
    }

    @Test
    public void testMultiCoreVM() {
        Job j1 = new Job(1000, cloudsim.clock());
        Job j2 = new Job(1000, cloudsim.clock());

        VM vm = new VM(100, 2, 100, 0.40, cloudsim);

        VMDriver driver = new VMDriver(vm, cloudsim);
        driver.setJobs(new Job[] { j1, j2 });

        cloudsim.startSimulation();

        assertEquals(j1.getReleaseTime(), 0.0, 0.0);
        assertEquals(j1.getSubmitTime(), 0.0, 0.0);
        assertEquals(j1.getStartTime(), 0.0, 0.0);
        assertEquals(j1.getFinishTime(), 10.0, 0.0);

        assertEquals(j2.getReleaseTime(), 0.0, 0.0);
        assertEquals(j2.getSubmitTime(), 0.0, 0.0);
        assertEquals(j2.getStartTime(), 0.0, 0.0);
        assertEquals(j2.getFinishTime(), 10.0, 0.0);
    }
}
