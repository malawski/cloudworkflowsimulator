package cws.core.cloudsim;

import org.cloudbus.cloudsim.core.SimEntity;
import org.cloudbus.cloudsim.core.SimEvent;

public abstract class CWSSimEntity extends SimEntity {

    private CloudSimWrapper cloudsim;

    public CWSSimEntity(String name, CloudSimWrapper cloudsim) {
        super(name);
        this.cloudsim = cloudsim;
    }

    public CloudSimWrapper getCloudsim() {
        return cloudsim;
    }

    @Deprecated
    @Override
    protected void send(int entityId, double delay, int cloudSimTag) {
        super.send(entityId, delay, cloudSimTag);
    }

    @Deprecated
    @Override
    protected void send(int entityId, double delay, int cloudSimTag, Object data) {
        super.send(entityId, delay, cloudSimTag, data);
    }

    @Deprecated
    @Override
    protected void send(String entityName, double delay, int cloudSimTag, Object data) {
        super.send(entityName, delay, cloudSimTag, data);
    }

    @Deprecated
    @Override
    protected void send(String entityName, double delay, int cloudSimTag) {
        super.send(entityName, delay, cloudSimTag);
    }

    /**
     * @param ev - the received event.
     */
    public void processEvent(CWSSimEvent ev) {
        // Do nothing by default
    }

    @Deprecated
    @Override
    public final void processEvent(SimEvent ev) {
        processEvent(new CWSSimEvent(ev));
    }

    /**
     * This is overridden and does nothing by default, because almost everywhere we do nothing in this method.
     * @see SimEntity#startEntity()
     */
    @Override
    public void startEntity() {
        // Do nothing by default
    }

    /**
     * This is overridden and does nothing by default, because almost everywhere we do nothing in this method.
     * @see SimEntity#shutdownEntity()
     */
    @Override
    public void shutdownEntity() {
        // Do nothing by default
    }
}
