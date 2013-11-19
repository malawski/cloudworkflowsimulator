package cws.core;

public class VMStaticParams implements Cloneable {
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

    public VMStaticParams setMips(int mips) {
        this.mips = mips;
        return this;
    }

    public int getCores() {
        return cores;
    }

    public double getPrice() {
        return price;
    }

    public VMStaticParams setCores(int cores) {
        this.cores = cores;
        return this;
    }

    public VMStaticParams setPrice(double price) {
        this.price = price;
        return this;
    }

    /**
     * Extracted defaults appearing in many places throughout the project
     * @return
     */
    public static VMStaticParams getDefaults() {
        return new VMStaticParams().setMips(1000).setCores(1).setPrice(1.0);
    }
}
