package cws.core.scheduler;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import cws.core.Cloud;
import cws.core.EnsembleManager;
import cws.core.Provisioner;
import cws.core.Scheduler;
import cws.core.SimpleJobFactory;
import cws.core.VM;
import cws.core.WorkflowEngine;
import cws.core.WorkflowEvent;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.dag.Task;
import cws.core.dag.DAG;
import cws.core.dag.DAGParser;
import cws.core.log.WorkflowLog;

public class DAGSchedulerFCFSTest {
    private CloudSimWrapper cloudsim;

    @Before
    public void setUp() {
        // TODO(_mequrel_): change to IoC in the future or to mock
        cloudsim = new CloudSimWrapper();
        cloudsim.init(1, null, false);
    }

    @Test
    public void testScheduleVMS() {
        Provisioner provisioner = null;
        Scheduler scheduler = new DAGSchedulerFCFS(cloudsim);
        WorkflowEngine engine = new WorkflowEngine(new SimpleJobFactory(1000), provisioner, scheduler, cloudsim);
        Cloud cloud = new Cloud(cloudsim);

        HashSet<VM> vms = new HashSet<VM>();
        for (int i = 0; i < 10; i++) {
            VM vm = new VM(1000, 1, 1.0, 1.0, cloudsim);
            vm.setProvisioningDelay(0.0);
            vm.setDeprovisioningDelay(0.0);
            vms.add(vm);
            cloudsim.send(engine.getId(), cloud.getId(), 0.1, WorkflowEvent.VM_LAUNCH, vm);
        }

        cloudsim.startSimulation();

        assertEquals(vms.size(), engine.getAvailableVMs().size());

    }

    @Test
    public void testScheduleDag() {
        Provisioner provisioner = null;
        Scheduler scheduler = new DAGSchedulerFCFS(cloudsim);
        WorkflowEngine engine = new WorkflowEngine(new SimpleJobFactory(1000), provisioner, scheduler, cloudsim);
        Cloud cloud = new Cloud(cloudsim);

        WorkflowLog jobLog = new WorkflowLog(cloudsim);

        engine.addJobListener(jobLog);

        HashSet<VM> vms = new HashSet<VM>();
        for (int i = 0; i < 10; i++) {
            VM vm = new VM(1000, 1, 1.0, 1.0, cloudsim);
            vms.add(vm);
            cloudsim.send(engine.getId(), cloud.getId(), 0.0, WorkflowEvent.VM_LAUNCH, vm);
        }

        DAG dag = new DAG();
        for (int i = 0; i < 100; i++) {
            Task task = new Task("TASK" + i, "transformation", (i % 10));
            dag.addTask(task);
        }

        List<DAG> dags = new ArrayList<DAG>();
        dags.add(dag);

        // FIXME (_mequrel): looks awkward, a comment should be added or some logic inversed
        new EnsembleManager(dags, engine, cloudsim);

        cloudsim.startSimulation();

        assertEquals(vms.size(), engine.getAvailableVMs().size());
        assertEquals(0, engine.getQueuedJobs().size());

        jobLog.printJobs("testScheduleDag");
    }

    @Test
    public void testScheduleDag100() {
        Provisioner provisioner = null;
        Scheduler scheduler = new DAGSchedulerFCFS(cloudsim);
        WorkflowEngine engine = new WorkflowEngine(new SimpleJobFactory(1000), provisioner, scheduler, cloudsim);
        Cloud cloud = new Cloud(cloudsim);

        WorkflowLog jobLog = new WorkflowLog(cloudsim);

        engine.addJobListener(jobLog);

        HashSet<VM> vms = new HashSet<VM>();
        for (int i = 0; i < 10; i++) {
            VM vm = new VM(1000, 1, 1.0, 1.0, cloudsim);
            vms.add(vm);
            cloudsim.send(engine.getId(), cloud.getId(), 0.0, WorkflowEvent.VM_LAUNCH, vm);
        }

        DAG dag = DAGParser.parseDAG(new File("dags/CyberShake_100.dag"));

        List<DAG> dags = new ArrayList<DAG>();
        dags.add(dag);

        // FIXME (_mequrel): looks awkward, a comment should be added or some logic inversed
        new EnsembleManager(dags, engine, cloudsim);

        cloudsim.startSimulation();

        assertEquals(vms.size(), engine.getAvailableVMs().size());
        assertEquals(0, engine.getQueuedJobs().size());

        jobLog.printJobs("testScheduleDag_CyberShake_100");
    }

}
