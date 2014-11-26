package cws.core.dag;

/**
 * A file consists of its name and size. It's immutable.
 */
public class DAGFile {
    /** DAG-wide unique file name. */
    private final String name;
    /** Size in bytes. */
    private final long size;
    /** The DAG this file belongs to. */
    private final DAG dag;

    public DAGFile(String name, long size, DAG dag) {
        this.name = name;
        this.size = size;
        this.dag = dag;
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
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        DAGFile other = (DAGFile) obj;
        if (dag == null) {
            if (other.dag != null)
                return false;
        } else if (!dag.equals(other.dag))
            return false;
        if (name == null) {
            if (other.name != null)
                return false;
        } else if (!name.equals(other.name))
            return false;
        return true;
    }

    /**
     * File name is globally unique.
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((dag == null) ? 0 : dag.hashCode());
        result = prime * result + ((name == null) ? 0 : name.hashCode());
        return result;
    }

    @Override
    public String toString() {
        return "DAGFile [name=" + name + ", size=" + size + ", dag=" + dag + "]";
    }
}
