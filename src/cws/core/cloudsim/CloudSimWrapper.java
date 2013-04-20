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

    public double clock() {
        return CloudSim.clock();
    }

    public void cancelAll(int src, Predicate p) {
        CloudSim.cancelAll(src, p);
    }

    public void init(int numUser, Calendar calendar, boolean traceFlag) {
        CloudSim.init(numUser, calendar, traceFlag);
    }

    public void startSimulation() {
        CloudSim.startSimulation();
    }

    public void send(int src, int dest, double delay, int tag, Object data) {
        CloudSim.send(src, dest, delay, tag, data);
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

}
