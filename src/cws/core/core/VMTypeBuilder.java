package cws.core.core;

public class VMTypeBuilder {
    private static final double DEFAULT_BILLING_TIME = 3600;

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

        VMType build();
    }

    static class Steps implements MipsStep, CoresStep, PriceStep, OptionalsStep {
        private int mips;
        private int cores;
        private double price;
        private double billingTimeInSeconds = DEFAULT_BILLING_TIME;

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
        public VMType build() {
            return new VMType(mips, cores, price, billingTimeInSeconds);
        }
    }
}
