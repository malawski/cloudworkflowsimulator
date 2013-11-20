package cws.core.core;

public class VMTypeBuilder {
    private static final double DEFAULT_BILLING_TIME = 3600;

    private int mips;
    private int cores;
    private double price;
    private double billingTimeInSeconds = DEFAULT_BILLING_TIME;

    public VMTypeBuilder mips(int mips) {
        this.mips = mips;
        return this;
    }

    public VMTypeBuilder cores(int cores) {
        this.cores = cores;
        return this;
    }

    public VMTypeBuilder price(double price) {
        this.price = price;
        return this;
    }

    public VMTypeBuilder billingTimeInSeconds(double billingTimeInSeconds) {
        this.billingTimeInSeconds = billingTimeInSeconds;
        return this;
    }

    public VMType build() {
        return new VMType(mips, cores, price, billingTimeInSeconds);
    }
}
