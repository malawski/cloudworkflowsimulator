package cws.core.provisioner;

import org.junit.Before;
import org.junit.Test;
import static org.mockito.Mockito.*;

import cws.core.Cloud;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.WorkflowEngine;

public class NullProvisionerTest {

    private CloudSimWrapper cloudsim;

    @Before
        public void setUp() {
        cloudsim = new CloudSimWrapper();
        cloudsim.init();
    }

    // This is a trivial test but it shows how Provisioners can be tested
    @Test
    public void testProvisionResourcesDoesNothing() {
        NullProvisioner np = new NullProvisioner(cloudsim);
        WorkflowEngine engine = mock(WorkflowEngine.class);
        Cloud c = mock(Cloud.class);
        np.setCloud(c);

        np.provisionResources(engine);

        // Check that nothing happened to the Cloud (i.e. no VMs were
        // launched or terminated).
        verifyZeroInteractions(c);
    }
}
