package cws.core.scheduler;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import cws.core.Cloud;
import cws.core.EnsembleManager;
import cws.core.Provisioner;
import cws.core.VM;
import cws.core.WorkflowEngine;
import cws.core.WorkflowEvent;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.core.VMType;
import cws.core.core.VMTypeBuilder;
import cws.core.dag.DAG;
import cws.core.dag.DAGParser;
import cws.core.engine.Environment;
import cws.core.engine.StorageAwarePredictionStrategy;
import cws.core.jobs.SimpleJobFactory;
import cws.core.log.WorkflowLog;
import cws.core.storage.StorageManager;
import cws.core.storage.cache.FIFOCacheManager;
import cws.core.storage.cache.VMCacheManager;
import cws.core.storage.global.GlobalStorageManager;
import cws.core.storage.global.GlobalStorageParams;

public class DAGDynamicSchedulerStorageAwareTest {
    private CloudSimWrapper cloudsim;
    private Provisioner provisioner;
    private DAGDynamicScheduler scheduler;
    private WorkflowEngine engine;
    private Cloud cloud;
    private WorkflowLog jobLog;
    private StorageManager storageManager;
    private Environment environment;

    @Before
    public void setUp() {
        cloudsim = new CloudSimWrapper();
        cloudsim.init();

        GlobalStorageParams params = new GlobalStorageParams();
        params.setReadSpeed(2000000.0);
        params.setWriteSpeed(1000000.0);

        VMCacheManager cacheManager = new FIFOCacheManager(cloudsim);
        storageManager = new GlobalStorageManager(params, cacheManager, cloudsim);
        environment = new Environment(VMTypeBuilder.DEFAULT_VM_TYPE, storageManager,
                new StorageAwarePredictionStrategy());

        provisioner = null;
        scheduler = new DAGDynamicScheduler(cloudsim);
        scheduler.setEnvironment(environment);
        engine = new WorkflowEngine(new SimpleJobFactory(), provisioner, scheduler, Double.MAX_VALUE, Double.MAX_VALUE,
                cloudsim);
        cloud = new Cloud(cloudsim);

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

        VM vm = new VM(vmType, cloudsim);
        cloudsim.send(engine.getId(), cloud.getId(), 0.0, WorkflowEvent.VM_LAUNCH, vm);
    }
}
