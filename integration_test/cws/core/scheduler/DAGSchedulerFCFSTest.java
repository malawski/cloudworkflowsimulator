package cws.core.scheduler;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import cws.core.*;
import cws.core.algorithms.VMType;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.dag.DAG;
import cws.core.dag.DAGParser;
import cws.core.dag.Task;
import cws.core.jobs.SimpleJobFactory;
import cws.core.log.WorkflowLog;
import cws.core.storage.VoidStorageManager;

public class DAGSchedulerFCFSTest {
    private CloudSimWrapper cloudsim;
    private Provisioner provisioner;
    private DAGSchedulerFCFS scheduler;
    private WorkflowEngine engine;
    private Cloud cloud;
    private WorkflowLog jobLog;

    @Before
    public void setUp() {
        // TODO(_mequrel_): change to IoC in the future or to mock
        cloudsim = new CloudSimWrapper();
        cloudsim.init();

        // TODO(bryk): that's ugly, I know
        new VoidStorageManager(cloudsim);
        provisioner = null;
        scheduler = new DAGSchedulerFCFS(cloudsim);
        engine = new WorkflowEngine(new SimpleJobFactory(1000), provisioner, scheduler, cloudsim);
        cloud = new Cloud(cloudsim);

        jobLog = new WorkflowLog(cloudsim);
        engine.addJobListener(jobLog);
    }

    @Test
    public void testScheduleVMS() {
        HashSet<VM> vms = new HashSet<VM>();
        for (int i = 0; i < 10; i++) {
            VMStaticParams vmStaticParams = VMStaticParams.getDefaults();

            VM vm = new VM(vmStaticParams, cloudsim);
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
        HashSet<VM> vms = new HashSet<VM>();
        for (int i = 0; i < 10; i++) {
            VMStaticParams vmStaticParams = VMStaticParams.getDefaults();

            VM vm = new VM(vmStaticParams, cloudsim);
            vms.add(vm);
            cloudsim.send(engine.getId(), cloud.getId(), 0.0, WorkflowEvent.VM_LAUNCH, vm);
        }

        DAG dag = new DAG();
        for (int i = 0; i < 100; i++) {
            Task task = new Task("TASK" + i, "transformation", (i % 10), VMType.DEFAULT_VM_TYPE);
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
        HashSet<VM> vms = new HashSet<VM>();
        for (int i = 0; i < 10; i++) {
            VMStaticParams vmStaticParams = VMStaticParams.getDefaults();

            VM vm = new VM(vmStaticParams, cloudsim);
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
