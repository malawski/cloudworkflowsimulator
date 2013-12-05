package cws.core.scheduler;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import cws.core.*;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.core.VMType;
import cws.core.core.VMTypeBuilder;
import cws.core.dag.DAG;
import cws.core.dag.DAGParser;
import cws.core.dag.Task;
import cws.core.engine.Environment;
import cws.core.jobs.SimpleJobFactory;
import cws.core.log.WorkflowLog;
import cws.core.storage.StorageManager;
import cws.core.storage.VoidStorageManager;

public class EnsembleDynamicSchedulerTest {
    private CloudSimWrapper cloudsim;
    private Provisioner provisioner;
    private EnsembleDynamicScheduler scheduler;
    private WorkflowEngine engine;
    private Cloud cloud;
    private WorkflowLog jobLog;
    private StorageManager storageManager;
    private Environment environment;

    @Before
    public void setUp() {
        // TODO(_mequrel_): change to IoC in the future or to mock
        cloudsim = new CloudSimWrapper();
        cloudsim.init();

        storageManager = new VoidStorageManager(cloudsim);
        environment = new Environment(VMTypeBuilder.newBuilder().mips(1).cores(1).price(1.0).build(), storageManager);

        provisioner = null;
        scheduler = new EnsembleDynamicScheduler(cloudsim);
        scheduler.setEnvironment(environment);
        engine = new WorkflowEngine(new SimpleJobFactory(1000), provisioner, scheduler, cloudsim);
        cloud = new Cloud(cloudsim);

        jobLog = new WorkflowLog(cloudsim);
        engine.addJobListener(jobLog);
    }

    @Test
    public void testScheduleVMS() {
        HashSet<VM> vms = new HashSet<VM>();
        for (int i = 0; i < 10; i++) {
            VMType vmType = VMTypeBuilder.newBuilder().mips(1).cores(1).price(1.0).build();

            VM vm = new VM(vmType, cloudsim);
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
            VMType vmType = VMTypeBuilder.newBuilder().mips(1).cores(1).price(1.0).build();

            VM vm = new VM(vmType, cloudsim);
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

        jobLog.printJobs();
    }

    @Test
    public void testScheduleDag100() {
        HashSet<VM> vms = new HashSet<VM>();
        for (int i = 0; i < 10; i++) {
            VMType vmType = VMTypeBuilder.newBuilder().mips(1).cores(1).price(1.0).build();

            VM vm = new VM(vmType, cloudsim);
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

        jobLog.printJobs();
    }

    @Test
    public void testScheduleDag100x10() {
        HashSet<VM> vms = new HashSet<VM>();
        for (int i = 0; i < 10; i++) {
            VMType vmType = VMTypeBuilder.newBuilder().mips(1).cores(1).price(1.0).build();
            VM vm = new VM(vmType, cloudsim);

            vms.add(vm);
            cloudsim.send(engine.getId(), cloud.getId(), 0.0, WorkflowEvent.VM_LAUNCH, vm);
        }

        List<DAG> dags = new ArrayList<DAG>();

        for (int i = 0; i < 10; i++) {
            DAG dag = DAGParser.parseDAG(new File("dags/CyberShake_100.dag"));
            dags.add(dag);
        }

        // FIXME (_mequrel): looks awkward, a comment should be added or some logic inversed
        new EnsembleManager(dags, engine, cloudsim);

        cloudsim.startSimulation();

        assertEquals(vms.size(), engine.getAvailableVMs().size());
        assertEquals(0, engine.getQueuedJobs().size());

        jobLog.printJobs();
    }
}
