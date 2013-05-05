package cws.core.dag;

/**
 * A file consists of its name and size.
 */
public class DAGFile {
    private String name;
    private double size;

    public DAGFile(String name, double size) {
        this.name = name;
        this.size = size;
    }

    public String getName() {
        return name;
    }

    public double getSize() {
        return size;
    }

    /**
     * File name is globally unique.
     */
    @Override
    public boolean equals(Object obj) {
        return obj instanceof DAGFile && ((DAGFile) obj).name.equals(name);
    }

    /**
     * File name is globally unique.
     */
    @Override
    public int hashCode() {
        return name.hashCode();
    }
}
