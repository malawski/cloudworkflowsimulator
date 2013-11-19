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
     * Price per hour of usage
     */
    private double price;

    public int getMips() {
        return mips;
    }

    public int getCores() {
        return cores;
    }

    public double getPrice() {
        return price;
    }

    public VMType(int mips, int cores, double price) {
        this.mips = mips;
        this.cores = cores;
        this.price = price;
    }
}
