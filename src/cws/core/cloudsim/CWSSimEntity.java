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

    public abstract void processEvent(CWSSimEvent ev);
    
    @Override
    public final void processEvent(SimEvent ev) {
        processEvent(new CWSSimEvent(ev));
    }
}

