package cws.core.algorithms;

public enum VMType {
    SMALL(1, 1.0), MEDIUM(5, 0.40), LARGE(10, 0.80);

    public final int mips;
    public final double price;

    VMType(int mips, double price) {
        this.mips = mips;
        this.price = price;
    }
}
