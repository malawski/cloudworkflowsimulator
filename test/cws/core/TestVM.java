package cws.core;

import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.SimEntity;
import org.cloudbus.cloudsim.core.SimEvent;
import org.junit.Test;

import cws.core.dag.Task;
import static org.junit.Assert.*;

public class TestVM {

    private class VMDriver extends SimEntity implements WorkflowEvent {
        private VM vm;
        private Job[] jobs;

        public VMDriver(VM vm) {
            super("VMDriver");
            this.vm = vm;
            CloudSim.addEntity(this);
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
        public void processEvent(SimEvent ev) {
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

    @Test
    public void testSingleJob() {
        CloudSim.init(1, null, false);

        Job j = new Job();
        j.setTask(new Task("task_id", "transformation", 1000));

        VM vm = new VM(100, 1, 100, 0.40);

        VMDriver driver = new VMDriver(vm);
        driver.setJobs(new Job[] { j });

        CloudSim.startSimulation();

        assertEquals(0.0, j.getReleaseTime(), 0.0);
        assertEquals(0.0, j.getSubmitTime(), 0.0);
        assertEquals(0.0, j.getStartTime(), 0.0);
        assertEquals(10.0, j.getFinishTime(), 0.0);
    }

    @Test
    public void testTwoJobs() {
        CloudSim.init(1, null, false);

        Job j1 = new Job();
        j1.setTask(new Task("task_id", "transformation", 1000));
        Job j2 = new Job();
        j2.setTask(new Task("task_id2", "transformation", 1000));

        VM vm = new VM(100, 1, 100, 0.40);

        VMDriver driver = new VMDriver(vm);
        driver.setJobs(new Job[] { j1, j2 });

        CloudSim.startSimulation();

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
        CloudSim.init(1, null, false);

        Job j1 = new Job();
        j1.setTask(new Task("task_id1", "transformation", 1000));

        Job j2 = new Job();
        j2.setTask(new Task("task_id2", "transformation", 1000));

        VM vm = new VM(100, 2, 100, 0.40);

        VMDriver driver = new VMDriver(vm);
        driver.setJobs(new Job[] { j1, j2 });

        CloudSim.startSimulation();

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
