package cws.core.cloudsim;

import java.io.OutputStream;
import java.io.PrintStream;
import java.util.Calendar;

import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.predicates.Predicate;

/**
 * Wrapper for CloudSim class. Why did we need this class? Because CloudSim has only static methods and we wanted to
 * make code more testable, hence we have created this class.
 */
public class CloudSimWrapper {
    /** Simulation wall start time in nanos */
    private long simulationStartWallTime;

    /** Simulation wall finish time in nanos */
    private long simulationFinishWallTime;

    /** The input stream to write logs to */
    private PrintStream logPrintStream;

    /** Whether logging is enabled. Defaults to true. */
    private boolean logsEnabled = true;

    /** The last time a log has been prited. */
    private double lastTime = 0.0;

    /**
     * Creates CloudSimWrapper which prints logs to stdout.
     */
    public CloudSimWrapper() {
        logPrintStream = System.out;
    }

    /**
     * Creates CloudSimWrapper which prints logs to the provided stream.
     * @param logOutputStream The stream to print logs to.
     */
    public CloudSimWrapper(OutputStream logOutputStream) {
        this.logPrintStream = new PrintStream(logOutputStream);
    }

    /**
     * @see CloudSim#clock()
     */
    public double clock() {
        return CloudSim.clock();
    }

    /**
     * @see CloudSim#cancelAll(int, Predicate)
     */
    public void cancelAll(int src, Predicate p) {
        CloudSim.cancelAll(src, p);
    }

    /**
     * Calls {@link CloudSim#init(int, Calendar, boolean)} with params 1, null, false.
     * @see CloudSim#init(int, Calendar, boolean)
     */
    public void init() {
        CloudSim.init(1, null, false);
    }

    /**
     * @see CloudSim#startSimulation()
     */
    public void startSimulation() {
        simulationStartWallTime = System.nanoTime();
        CloudSim.startSimulation();
        simulationFinishWallTime = System.nanoTime();
    }

    /**
     * @see CloudSim#getEntityId(String)
     */
    public int getEntityId(String entityName) {
        return CloudSim.getEntityId(entityName);
    }

    /**
     * @see CloudSim#getEntity(String)
     */
    public Object getEntityByName(String name) {
        return CloudSim.getEntity(name);
    }

    /**
     * @see CloudSim#send(int, int, double, int, Object)
     */
    public void send(int src, int dest, double delay, int tag, Object data) {
        CloudSim.send(src, dest, delay, tag, data);
    }

    /**
     * Calls {@link #send(int, int, double, int, Object)} with null data
     */
    public void send(int src, int dest, double delay, int tag) {
        send(src, dest, delay, tag, null);
    }

    /**
     * Calls {@link #send(int, int, double, int, Object)} with 0.0 delay.
     */
    public void sendNow(int src, int dest, int tag, Object data) {
        send(src, dest, 0.0, tag, data);
    }

    /**
     * Calls {@link #send(int, int, double, int, Object)} with myself.getId() as src and dest.
     */
    public void sendToMyself(CWSSimEntity myslef, double delay, int tag, Object data) {
        send(myslef.getId(), myslef.getId(), delay, tag, data);
    }

    /**
     * Logs the given message to previously set output stream.
     * @param msg The message to logs.
     */
    public void log(String msg) {
        if (logsEnabled) {
            if (CloudSim.running()) {
                logPrintStream.println((clock() - lastTime) + " (" + clock() + ") " + msg);
                lastTime = clock();
            } else {
                logPrintStream.println(msg);
            }
        }
    }

    /**
     * @param logsEnabled Whether logging should be enabled.
     */
    public void setLogsEnabled(boolean logsEnabled) {
        this.logsEnabled = logsEnabled;
    }

    /**
     * @return Simulation wall time in nanos.
     */
    public double getSimulationWallTime() {
        return simulationFinishWallTime - simulationStartWallTime;
    }
}
