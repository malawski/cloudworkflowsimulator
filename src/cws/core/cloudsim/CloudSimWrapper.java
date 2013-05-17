package cws.core.cloudsim;

import java.util.Calendar;

import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.SimEntity;
import org.cloudbus.cloudsim.core.predicates.Predicate;

public class CloudSimWrapper {

    public void addEntity(SimEntity entity) {
        CloudSim.addEntity(entity);
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

    private double lastTime = 0.0;

    public void log(String msg) {
        Log.printLine((clock() - lastTime) + " (" + clock() + ") " + msg);
        lastTime = clock();
    }

    public void disableLogging() {
        Log.disable();
    }

    public void print(String string) {
        Log.print(string);
    }
}
