package cws.core.scheduler;

import java.io.File;
import java.util.*;

import cws.core.pricing.PricingConfigLoader;
import cws.core.pricing.PricingManager;
import cws.core.pricing.PricingModelFactory;
import org.junit.Before;
import org.junit.Test;

import cws.core.Cloud;
import cws.core.EnsembleManager;
import cws.core.Provisioner;
import cws.core.VM;
import cws.core.VMFactory;
import cws.core.WorkflowEngine;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.core.VMType;
import cws.core.core.VMTypeBuilder;
import cws.core.dag.DAG;
import cws.core.dag.DAGParser;
import cws.core.engine.Environment;
import cws.core.log.WorkflowLog;
import cws.core.provisioner.NullProvisioner;
import cws.core.storage.StorageManager;
import cws.core.storage.cache.FIFOCacheManager;
import cws.core.storage.cache.VMCacheManager;
import cws.core.storage.global.GlobalStorageManager;
import cws.core.storage.global.GlobalStorageParams;


public class DAGDynamicSchedulerStorageAwareTest {
    private VMType vmType;
    private CloudSimWrapper cloudsim;
    private Provisioner provisioner;
    private DAGDynamicScheduler scheduler;
    private WorkflowEngine engine;
    private Cloud cloud;
    private WorkflowLog jobLog;
    private StorageManager storageManager;
    private PricingManager pricingManager;
    private Environment environment;

    @Before
    public void setUp() {
        cloudsim = new CloudSimWrapper();
        cloudsim.init();

        GlobalStorageParams params = new GlobalStorageParams();
        params.setReadSpeed(2000000.0);
        params.setWriteSpeed(1000000.0);

        Map<String, Object> pricingParams = new HashMap<String, Object>();
        pricingParams.put(PricingConfigLoader.MODEL_ENTRY, "simple");
        pricingParams.put(PricingConfigLoader.BILLING_TIME_ENTRY, 60);
        PricingManager pricingManager = new PricingManager(PricingModelFactory.getPricingModel(pricingParams));

        VMCacheManager cacheManager = new FIFOCacheManager(cloudsim);
        storageManager = new GlobalStorageManager(params, cacheManager, cloudsim);
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
    public void shouldTakeIntoConsiderationTransferMakespans() {
        /**
         * input DAG:
         *
         * .....| in.txt 100B = 50s read
         * .....|
         * ...ID000 2s
         * .....|
         * .....| mid.txt 1000B = 500s read, 1000s write
         * .....|
         * ...ID001 40s
         * .....|
         * .....| out.txt 200B = 200s write
         *
         */

        launchVM();
        List<DAG> dags = loadTestDAG("dags/storage_integration_tests/simpleSequence.dag");
        startSimulation(dags);

        /**
         * expected:
         *
         * ... 50.0 Read in.txt
         * .... 2.0 Run job ID000
         * . 1000.0 Write mid.txt
         * .. 500.0 Read mid.txt
         * ... 40.0 Run job ID001
         * .. 200.0 Write out.txt
         */
    }

    protected void startSimulation(List<DAG> dags) {
        new EnsembleManager(dags, engine, cloudsim);

        cloudsim.startSimulation();
    }

    protected List<DAG> loadTestDAG(String file) {
        DAG dag = DAGParser.parseDAG(new File(file));

        List<DAG> dags = new ArrayList<DAG>();
        dags.add(dag);
        return dags;
    }

    protected void launchVM() {
        VMType vmType = VMTypeBuilder.newBuilder().mips(1).cores(1).price(1.0).build();

        VM vm = VMFactory.createVM(vmType, cloudsim);
        provisioner.launchVM(vm);
    }
}
