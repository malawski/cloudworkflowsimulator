package cws.core.core;

public class VMType implements Cloneable {
    /**
     * The processing power of this VM
     */
    public int mips;

    /**
     * The number of cores of this VM
     */
    public int cores;

    /**
     * Price per hour of usage
     */
    public double price;

    public int getMips() {
        return mips;
    }

    public VMType setMips(int mips) {
        this.mips = mips;
        return this;
    }

    public int getCores() {
        return cores;
    }

    public double getPrice() {
        return price;
    }

    public VMType setCores(int cores) {
        this.cores = cores;
        return this;
    }

    public VMType setPrice(double price) {
        this.price = price;
        return this;
    }

}
