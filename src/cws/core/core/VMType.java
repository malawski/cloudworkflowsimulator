package cws.core.core;

public class VMType implements Cloneable {
    /**
     * The processing power of this VM
     */
    private int mips;

    /**
     * The number of cores of this VM
     */
    private int cores;

    /**
     * Price per billing unit of usage
     */
    private double billingUnitPrice;

    /**
     * For how long we pay in advance
     */
    private double billingTimeInSeconds;

    public int getMips() {
        return mips;
    }

    public int getCores() {
        return cores;
    }

    public double getPriceForBillingUnit() {
        return billingUnitPrice;
    }

    public double getBillingTimeInSeconds() {
        return billingTimeInSeconds;
    }

    public VMType(int mips, int cores, double billingUnitPrice, double billingTimeInSeconds) {
        this.mips = mips;
        this.cores = cores;
        this.billingUnitPrice = billingUnitPrice;
        this.billingTimeInSeconds = billingTimeInSeconds;
    }
}
