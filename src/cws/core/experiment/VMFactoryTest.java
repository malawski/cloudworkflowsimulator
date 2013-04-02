package cws.core.experiment;

import static org.junit.Assert.*;

import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.distributions.ContinuousDistribution;
import org.cloudbus.cloudsim.distributions.LognormalDistr;
import org.junit.Test;

import cws.core.VM;

public class VMFactoryTest {

    @Test
    public void testCreateVM() {

        CloudSim.init(1, null, false);
        // To get mean m and stddev s, use:
        // sigma = sqrt(log(1+s^2/m^2))
        // mu = log(m)-0.5*log(1+s^2/m^2)
        // For mean = 60 and stddev = 10: mu = 4.080645 sigma = 0.1655264
        // For mean = 20 and stddev = 5: mu = 2.96542, sigma = 0.09975135

        ContinuousDistribution provisioningDelayDistribution = new LognormalDistr(new java.util.Random(0), 4.080645,
                0.1655264);
        ContinuousDistribution deprovisioningDelayDistribution = new LognormalDistr(new java.util.Random(0), 2.96542,
                0.09975135);

        VMFactory.setProvisioningDelayDistribution(provisioningDelayDistribution);
        VMFactory.setDeprovisioningDelayDistribution(deprovisioningDelayDistribution);

        for (int i = 0; i < 1000; i++) {
            VM vm = VMFactory.createVM(1000, 1, 1.0, 1.0);
            assertTrue(vm.getProvisioningDelay() > 0.0);
            System.out.println(vm.getProvisioningDelay());
        }
        for (int i = 0; i < 1000; i++) {
            VM vm = VMFactory.createVM(1000, 1, 1.0, 1.0);
            assertTrue(vm.getDeprovisioningDelay() > 0.0);
            System.out.println(vm.getDeprovisioningDelay());
        }
    }

}
