package cws.core.core;

import org.cloudbus.cloudsim.distributions.ContinuousDistribution;

import cws.core.provisioner.ConstantDistribution;

public class VMTypeBuilder {
    private static final double DEFAULT_BILLING_TIME = 3600;
    private static final long DEFAULT_CACHE_SIZE = 100000000;

    private static final ContinuousDistribution DEFAULT_PROVISIONING_DELAY = new ConstantDistribution(0.0);
    private static final ContinuousDistribution DEFAULT_DEPROVISIONING_DELAY = new ConstantDistribution(10.0);

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

        OptionalsStep provisioningTime(ContinuousDistribution provisioningTime);

        OptionalsStep deprovisioningTime(ContinuousDistribution deprovisioningTime);

        OptionalsStep cacheSize(long cacheSize);

        VMType build();
    }

    static class Steps implements MipsStep, CoresStep, PriceStep, OptionalsStep {
        private int mips;
        private int cores;
        private double price;

        private double billingTimeInSeconds = DEFAULT_BILLING_TIME;
        private ContinuousDistribution provisioningTime = DEFAULT_PROVISIONING_DELAY;
        private ContinuousDistribution deprovisioningTime = DEFAULT_DEPROVISIONING_DELAY;
        private long cacheSize = DEFAULT_CACHE_SIZE;

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
        public OptionalsStep provisioningTime(ContinuousDistribution provisioningTime) {
            this.provisioningTime = provisioningTime;
            return this;
        }

        @Override
        public OptionalsStep deprovisioningTime(ContinuousDistribution deprovisioningTime) {
            // TODO(mequrel): add checks for >= 0.0 somewhere
            this.deprovisioningTime = deprovisioningTime;
            return this;
        }

        @Override
        public OptionalsStep cacheSize(long cacheSize) {
            this.cacheSize = cacheSize;
            return this;
        }

        @Override
        public VMType build() {
            return new VMType(mips, cores, price, billingTimeInSeconds, provisioningTime, deprovisioningTime, cacheSize);
        }
    }
}
