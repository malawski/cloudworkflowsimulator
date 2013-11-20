package cws.core.core;

import org.cloudbus.cloudsim.distributions.ContinuousDistribution;

import cws.core.provisioner.ConstantDistribution;

public class VMTypeBuilder {
    /**
     * Default VMType for simulations. It will be replaced once we introcude configurability.
     */
    public static final VMType DEFAULT_VM_TYPE = VMTypeBuilder.newBuilder().mips(1).cores(1).price(1.0).build();

    private static final double DEFAULT_BILLING_TIME = 3600;
    private static final double DEFAULT_PROVISIONING_DELAY = 0.0;
    private static final double DEFAULT_DEPROVISIONING_DELAY = 10.0;

    private static ContinuousDistribution provisioningDelayDistribution = new ConstantDistribution(
            DEFAULT_PROVISIONING_DELAY);
    private static ContinuousDistribution deprovisioningDelayDistribution = new ConstantDistribution(
            DEFAULT_DEPROVISIONING_DELAY);

    public static MipsStep newBuilder() {
        return new Steps();
    }

    public interface MipsStep {
        CoresStep mips(int mips);
    }

    public interface CoresStep {
        PriceStep cores(int cores);
    }

    public interface PriceStep {
        OptionalsStep price(double price);
    }

    public interface OptionalsStep {
        OptionalsStep billingTimeInSeconds(double billingTimeInSeconds);

        OptionalsStep provisioningTime(double provisioningTime);

        OptionalsStep deprovisioningTime(double deprovisioningTime);

        VMType build();
    }

    static class Steps implements MipsStep, CoresStep, PriceStep, OptionalsStep {
        private int mips;
        private int cores;
        private double price;
        private double billingTimeInSeconds = DEFAULT_BILLING_TIME;
        private double provisioningTime;
        private double deprovisioningTime;

        public Steps() {
            this.provisioningTime = provisioningDelayDistribution.sample();
            this.deprovisioningTime = deprovisioningDelayDistribution.sample();
        }

        @Override
        public CoresStep mips(int mips) {
            this.mips = mips;
            return this;
        }

        @Override
        public PriceStep cores(int cores) {
            this.cores = cores;
            return this;
        }

        @Override
        public OptionalsStep price(double price) {
            this.price = price;
            return this;
        }

        @Override
        public OptionalsStep billingTimeInSeconds(double billingTimeInSeconds) {
            this.billingTimeInSeconds = billingTimeInSeconds;
            return this;
        }

        @Override
        public OptionalsStep provisioningTime(double provisioningTime) {
            this.provisioningTime = provisioningTime;
            return this;
        }

        @Override
        public OptionalsStep deprovisioningTime(double deprovisioningTime) {
            this.deprovisioningTime = deprovisioningTime;
            return this;
        }

        @Override
        public VMType build() {
            return new VMType(mips, cores, price, billingTimeInSeconds, provisioningTime, deprovisioningTime);
        }
    }
}
