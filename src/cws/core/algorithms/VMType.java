package cws.core.algorithms;

public enum VMType {
    UNUSED_DEFAULT_VM_TYPE(1, 1.0);
    // NOTE(bryk): commented out the rest, since we don't use them
    // , MEDIUM(5, 0.40), LARGE(10, 0.80);

    private final int mips;
    private final double price;

    VMType(int mips, double price) {
        this.mips = mips;
        this.price = price;
    }

    public int getMips() {
        return mips;
    }

    public double getPrice() {
        return price;
    }
}
