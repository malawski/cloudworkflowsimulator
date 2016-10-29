package cws.core.scheduler;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.util.*;

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
import cws.core.log.WorkflowLog;
import cws.core.pricing.PricingConfigLoader;
import cws.core.pricing.PricingManager;
import cws.core.pricing.PricingModelFactory;
import cws.core.provisioner.NullProvisioner;
import cws.core.storage.StorageManager;
import cws.core.storage.VoidStorageManager;

public class DAGDynamicSchedulerTest {
    private CloudSimWrapper cloudsim;

    private Provisioner provisioner;

    private DAGDynamicScheduler scheduler;

    private WorkflowEngine engine;

    private Cloud cloud;

    private WorkflowLog jobLog;

    private StorageManager storageManager;

    private Environment environment;

    private VMType vmType;

    @Before
    public void setUp() {
        cloudsim = new CloudSimWrapper();
        cloudsim.init();

        Map<String, Object> pricingParams = new HashMap<String, Object>();
        pricingParams.put(PricingConfigLoader.MODEL_ENTRY, "simple");
        pricingParams.put(PricingConfigLoader.BILLING_TIME_ENTRY, 60);
        PricingManager pricingManager = new PricingManager(PricingModelFactory.getPricingModel(pricingParams));

        storageManager = new VoidStorageManager(cloudsim);
        vmType = VMTypeBuilder.newBuilder().mips(1).cores(1).price(1.0).build();
        environment = new Environment(Collections.singleton(vmType), storageManager, pricingManager);

        provisioner = new NullProvisioner(cloudsim);
        scheduler = new EnsembleDynamicScheduler(cloudsim, environment);
        engine = new WorkflowEngine(provisioner, scheduler, Double.MAX_VALUE, Double.MAX_VALUE, cloudsim, environment);
        cloud = new Cloud(cloudsim);
        provisioner.setCloud(cloud);
        cloud.addVMListener(engine);

        jobLog = new WorkflowLog(cloudsim);
        engine.addJobListener(jobLog);
    }

    @Test
    public void testScheduleDag() {
        HashSet<VM> vms = new HashSet<VM>();
        for (int i = 0; i < 10; i++) {
            VMType vmType = VMTypeBuilder.newBuilder().mips(1).cores(1).price(1.0).build();
            VM vm = VMFactory.createVM(vmType, cloudsim);
            vms.add(vm);
            provisioner.launchVM(vm);
        }

        DAG dag = new DAG();
        for (int i = 0; i < 100; i++) {
            Task task = new Task("TASK" + i, "transformation", (i % 10));
            dag.addTask(task);
        }

        List<DAG> dags = new ArrayList<DAG>();
        dags.add(dag);

        new EnsembleManager(dags, engine, cloudsim);

        cloudsim.startSimulation();

        assertEquals(vms.size(), engine.getAvailableVMs().size());
        assertEquals(0, engine.getAndClearReleasedJobs().size());

        jobLog.printJobs();
    }

    @Test
    public void testScheduleDag100() {
        HashSet<VM> vms = new HashSet<VM>();
        for (int i = 0; i < 10; i++) {
            VMType vmType = VMTypeBuilder.newBuilder().mips(1).cores(1).price(1.0).build();

            VM vm = VMFactory.createVM(vmType, cloudsim);
            vms.add(vm);
            provisioner.launchVM(vm);
        }

        DAG dag = DAGParser.parseDAG(new File("dags/CyberShake_100.dag"));

        List<DAG> dags = new ArrayList<DAG>();
        dags.add(dag);

        new EnsembleManager(dags, engine, cloudsim);

        cloudsim.startSimulation();

        assertEquals(vms.size(), engine.getAvailableVMs().size());
        assertEquals(0, engine.getAndClearReleasedJobs().size());

        jobLog.printJobs();
    }
}
