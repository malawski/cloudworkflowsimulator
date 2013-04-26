package cws.core.cloudsim;

import java.util.Calendar;

import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.SimEntity;
import org.cloudbus.cloudsim.core.predicates.Predicate;

public class CloudSimWrapper {
    private boolean isInitialized = false;

    public void addEntity(SimEntity entity) {
        CloudSim.addEntity(entity);
    }

    public double clock() {
        return CloudSim.clock();
    }

    public void cancelAll(int src, Predicate p) {
        CloudSim.cancelAll(src, p);
    }

    /**
     * Calls {@link CloudSim#init(int, Calendar, boolean)} with params 1, null, false.
     * It calls this method only once. If you want to reinitialize CloudSim you need to create a new CloudSimWrapper
     * instance.
     */
    public void init() {
        if (!isInitialized) {
            CloudSim.init(1, null, false);
            isInitialized = true;
        }
    }

    public void startSimulation() {
        CloudSim.startSimulation();
    }

    /**
     * @see CloudSim#getEntityId(String)
     */
    public int getEntityId(String entityName) {
        return CloudSim.getEntityId(entityName);
    }

    /**
     * @see CloudSim#send(int, int, double, int, Object)
     */
    public void send(int src, int dest, double delay, int tag, Object data) {
        CloudSim.send(src, dest, delay, tag, data);
    }

    public void send(int src, int dest, double delay, int tag) {
        send(src, dest, delay, tag, null);
    }

    public void log(String msg) {
        Log.printLine(clock() + msg);
    }

    public void disableLogging() {
        Log.disable();
    }

    public void print(String string) {
        Log.print(string);
    }

    /**
     * @return the isInitialized
     */
    public boolean isInitialized() {
        return isInitialized;
    }
}
