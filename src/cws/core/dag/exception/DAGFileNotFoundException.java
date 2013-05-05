package cws.core.dag.exception;

public class DAGFileNotFoundException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public DAGFileNotFoundException(String name) {
        super(name);
    }

}
