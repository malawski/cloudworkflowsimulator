package cws.core.core;

public class VMTypeFactory {

    /**
     * Extracted defaults appearing in many places throughout the project
     * @return
     */
    public static VMType getDefaults() {
        return new VMTypeBuilder().mips(1000).cores(1).price(1.0).build();
    }

    public static VMType fromOldVMType(cws.core.algorithms.VMType oldVMType) {
        return new VMTypeBuilder().mips(oldVMType.getMips()).cores(1).price(oldVMType.getPrice()).build();
    }
}
