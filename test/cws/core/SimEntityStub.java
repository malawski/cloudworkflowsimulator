package cws.core;

import org.cloudbus.cloudsim.core.SimEntity;
import org.cloudbus.cloudsim.core.SimEvent;

/**
 * Used in tests using SimEntities to save a few lines of code.
 */
public class SimEntityStub extends SimEntity {

    public SimEntityStub(String name) {
        super(name);
    }

    public SimEntityStub() {
        super("SimEntityStub");
    }

    /**
     * @see org.cloudbus.cloudsim.core.SimEntity#startEntity()
     */
    @Override
    public void startEntity() {
    }

    /**
     * @see org.cloudbus.cloudsim.core.SimEntity#processEvent(org.cloudbus.cloudsim.core.SimEvent)
     */
    @Override
    public void processEvent(SimEvent ev) {
    }

    /**
     * @see org.cloudbus.cloudsim.core.SimEntity#shutdownEntity()
     */
    @Override
    public void shutdownEntity() {
    }
}
