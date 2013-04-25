package cws.core.cloudsim;

import org.cloudbus.cloudsim.core.SimEntity;
import org.cloudbus.cloudsim.core.SimEvent;

public abstract class CWSSimEntity extends SimEntity {

    private CloudSimWrapper cloudsim;

    public CWSSimEntity(String name, CloudSimWrapper cloudsim) {
        super(name);
        this.cloudsim = cloudsim;
    }

    protected CloudSimWrapper getCloudsim() {
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

    public abstract void processEvent(CWSSimEvent ev);

    @Override
    public final void processEvent(SimEvent ev) {
        processEvent(new CWSSimEvent(ev));
    }
}
