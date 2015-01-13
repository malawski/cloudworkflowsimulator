package cws.core.provisioner;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.*;
import static org.hamcrest.CoreMatchers.*;

import cws.core.Cloud;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.WorkflowEngine;
import cws.core.VM;
import cws.core.core.VMType;
import cws.core.core.VMTypeBuilder;
import cws.core.VMFactory;

import cws.core.Provisioner;


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

    @Test
    public void testScheduleVMs() {
        Provisioner provisioner = new NullProvisioner(cloudsim);
        Cloud cloud = new Cloud(cloudsim);
        provisioner.setCloud(cloud);

        VMType vmType = VMTypeBuilder.newBuilder().mips(1).cores(1)
                    .price(1.0)
                    .provisioningTime(new ConstantDistribution(0.0))
                    .deprovisioningTime(new ConstantDistribution(0.0))
                    .build();

        final int nVMs = 10;
        for (int i = 0; i < nVMs; i++) {
            VM vm = VMFactory.createVM(vmType, cloudsim);
            provisioner.launchVM(vm);
        }

        cloudsim.startSimulation();

        assertThat("All VMs are allocated", cloud.getAllVMs().size(), is(nVMs));
    }

    @Test
    public void testScheduleVMsAtTime() {
        Provisioner provisioner = new NullProvisioner(cloudsim);
        Cloud cloud = new Cloud(cloudsim);
        provisioner.setCloud(cloud);

        VMType vmType = VMTypeBuilder.newBuilder().mips(1).cores(1)
                    .price(1.0)
                    .provisioningTime(new ConstantDistribution(0.0))
                    .deprovisioningTime(new ConstantDistribution(0.0))
                    .build();

        final int nVMs = 10;
        for (int i = 0; i < nVMs; i++) {
            VM vm = VMFactory.createVM(vmType, cloudsim);
            provisioner.launchVMAtTime(vm, 0.1);
        }

        cloudsim.startSimulation();

        assertThat("All VMs are allocated", cloud.getAllVMs().size(), is(nVMs));

        assertThat("VM is allocated at the correct time",
                cloud.getAllVMs().iterator().next().getLaunchTime(), is(0.1));
    }
}
