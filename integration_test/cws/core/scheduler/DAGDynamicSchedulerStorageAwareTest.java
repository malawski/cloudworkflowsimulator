package cws.core.scheduler;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import cws.core.*;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.dag.DAG;
import cws.core.dag.DAGParser;
import cws.core.jobs.SimpleJobFactory;
import cws.core.log.WorkflowLog;
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

    @Before
    public void setUp() {
        // TODO(_mequrel_): change to IoC in the future or to mock
        cloudsim = new CloudSimWrapper();
        cloudsim.init();

        GlobalStorageParams params = new GlobalStorageParams();
        params.setReadSpeed(2.0);
        params.setWriteSpeed(1.0);

        // XXX(bryk): note @mequrel that I've hardcoded FIFO cache here. Change this once you start refactoring this
        // code.
        VMCacheManager cacheManager = new FIFOCacheManager(cloudsim);
        // TODO(bryk): that's ugly, I know
        new GlobalStorageManager(params, cacheManager, cloudsim);
        provisioner = null;
        scheduler = new DAGDynamicScheduler(cloudsim);
        engine = new WorkflowEngine(new SimpleJobFactory(1000), provisioner, scheduler, cloudsim);
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

        // TODO(mequrel): should be converted into automatic assertion
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
        // FIXME (_mequrel): looks awkward, a comment should be added or some logic inversed
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
        VMStaticParams vmStaticParams = VMStaticParams.getDefaults();

        VM vm = new VM(vmStaticParams, cloudsim);
        cloudsim.send(engine.getId(), cloud.getId(), 0.0, WorkflowEvent.VM_LAUNCH, vm);
    }

}
