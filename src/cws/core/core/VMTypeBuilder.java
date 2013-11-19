package cws.core.core;

public class VMTypeBuilder {
    private int mips;
    private int cores;
    private double price;

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

    public VMType build() {
        return new VMType(mips, cores, price);
    }
}
