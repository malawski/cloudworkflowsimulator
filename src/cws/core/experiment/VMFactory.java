package cws.core.experiment;

import org.cloudbus.cloudsim.distributions.ContinuousDistribution;

import cws.core.IdentityRuntimeDistribution;
import cws.core.RuntimeDistribution;
import cws.core.VM;

public class VMFactory {
    
    static class ZeroDistribution implements ContinuousDistribution {
        @Override
        public double sample() {
            return 0.0d;
        }
    }
    
    private static ContinuousDistribution provisioningDelayDistribution = new ZeroDistribution();
    private static ContinuousDistribution deprovisioningDelayDistribution = new ZeroDistribution();
    private static RuntimeDistribution runtimeDistribution = new IdentityRuntimeDistribution();
    
    public static ContinuousDistribution getProvisioningDelayDistribution() {
        return VMFactory.provisioningDelayDistribution;
    }
    
    public static void setProvisioningDelayDistribution(ContinuousDistribution distribution) {
        VMFactory.provisioningDelayDistribution = distribution;
    }
    
    public static ContinuousDistribution getDeprovisioningDelayDistribution() {
        return VMFactory.deprovisioningDelayDistribution;
    }

    public static void setDeprovisioningDelayDistribution(
            ContinuousDistribution deprovisioningDelayDistribution) {
        VMFactory.deprovisioningDelayDistribution = deprovisioningDelayDistribution;
    }
    
    public static void setRuntimeDistribution(
            RuntimeDistribution runtimeDistribution) {
        VMFactory.runtimeDistribution = runtimeDistribution;
    }
    
    public static RuntimeDistribution getRuntimeDistribution() {
        return runtimeDistribution;
    }
    
    public static VM createVM(int mips, int cores, double bandwidth, double price) {
        VM vm = new VM(mips, cores, bandwidth, price);
        vm.setProvisioningDelay(provisioningDelayDistribution.sample());
        vm.setDeprovisioningDelay(deprovisioningDelayDistribution.sample());
        vm.setRuntimeDistribution(runtimeDistribution);
        return vm;
    }
}
