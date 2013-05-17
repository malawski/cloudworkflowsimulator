package cws.core.dag;

/**
 * A file consists of its name and size. It's immutable.
 */
public class DAGFile {
    private String name;
    private long size;

    public DAGFile(String name, long size) {
        this.name = name;
        this.size = size;
    }

    public String getName() {
        return name;
    }

    public long getSize() {
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
