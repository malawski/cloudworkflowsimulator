/**
 * 
 */
package cws.core.exception;

/**
 * Thrown when some SimEntity is ordered do process unknown SimEvent.
 */
public class UnknownWorkflowEventException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public UnknownWorkflowEventException(String msg) {
        super(msg);
    }
}
