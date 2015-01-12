package cws.core.provisioner;

import cws.core.cloudsim.CloudSimWrapper;
import cws.core.engine.Environment;

/** A provisioner base class for when only a single type of VM and single
 * StorageManager is in use (and so an Environment class can provide all
 * the information needed).
 */
public abstract class HomogeneousCloudAwareProvisioner extends CloudAwareProvisioner {

    protected Environment environment;

    public HomogeneousCloudAwareProvisioner (CloudSimWrapper cloudsim) {
        super(cloudsim);
    }

    public void setEnvironment(Environment environment) {
        this.environment = environment;
    }
}
