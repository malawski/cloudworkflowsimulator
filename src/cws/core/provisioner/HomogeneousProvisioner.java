package cws.core.provisioner;

import cws.core.Provisioner;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.engine.Environment;

/** A provisioner base class for when only a single type of VM and single
 * StorageManager is in use (and so an Environment class can provide all
 * the information needed).
 */
public abstract class HomogeneousProvisioner extends Provisioner {

    protected Environment environment;

    public HomogeneousProvisioner (CloudSimWrapper cloudsim) {
        super(cloudsim);
    }

    public void setEnvironment(Environment environment) {
        this.environment = environment;
    }
}
