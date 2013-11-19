package cws.core.core;

public class VMTypeFactory {

    /**
     * Extracted defaults appearing in many places throughout the project
     * @return
     */
    public static VMType getDefaults() {
        return new VMType().setMips(1000).setCores(1).setPrice(1.0);
    }
}
