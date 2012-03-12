package cws.core.experiment;


import org.cloudbus.cloudsim.distributions.ContinuousDistribution;
import org.cloudbus.cloudsim.distributions.LognormalDistr;

import cws.core.VM;

public class VMFactory {
	
    private static ContinuousDistribution provisioningDelayDistribution;
    private static ContinuousDistribution deprovisioningDelayDistribution;

    //= new LognormalDistr(new Random(0),DEFAULT_DEPROVISIONING_DELAY,10);

	public static ContinuousDistribution getProvisioningDelayDistribution() {
		return provisioningDelayDistribution;
	}

	public static void setProvisioningDelayDistribution(ContinuousDistribution distribution) {
		VMFactory.provisioningDelayDistribution = distribution;
	}
    
	public static ContinuousDistribution getDeprovisioningDelayDistribution() {
		return deprovisioningDelayDistribution;
	}

	public static void setDeprovisioningDelayDistribution(
			ContinuousDistribution deprovisioningDelayDistribution) {
		VMFactory.deprovisioningDelayDistribution = deprovisioningDelayDistribution;
	}	
	
	public static VM createVM(int mips, int cores, double bandwidth, double price) {
		VM vm = new VM(mips, cores, bandwidth, price);
        vm.setProvisioningDelay(provisioningDelayDistribution.sample());
        vm.setDeprovisioningDelay(deprovisioningDelayDistribution.sample());
        return vm;
	}


	

}
