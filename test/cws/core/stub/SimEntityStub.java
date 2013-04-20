package cws.core.stub;

import cws.core.cloudsim.CWSSimEntity;
import cws.core.cloudsim.CWSSimEvent;
import cws.core.cloudsim.CloudSimWrapper;

/**
 * Used in tests using SimEntities to save a few lines of code.
 */
public class SimEntityStub extends CWSSimEntity {

    public SimEntityStub(String name, CloudSimWrapper cloudsim) {
        super(name, cloudsim);
    }

    public SimEntityStub(CloudSimWrapper cloudsim) {
        super("SimEntityStub", cloudsim);
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
    public void processEvent(CWSSimEvent ev) {
    }

    /**
     * @see org.cloudbus.cloudsim.core.SimEntity#shutdownEntity()
     */
    @Override
    public void shutdownEntity() {
    }
}
