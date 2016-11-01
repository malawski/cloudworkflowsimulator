package cws.core.provisioner;

import com.google.common.base.Preconditions;
import cws.core.Provisioner;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.core.VMType;
import cws.core.engine.Environment;

/** A provisioner base class for when only a single type of VM and single
 * StorageManager is in use (and so an Environment class can provide all
 * the information needed).
 */
public abstract class HomogeneousProvisioner extends Provisioner {

    protected Environment environment;

    public HomogeneousProvisioner (CloudSimWrapper cloudsim, Environment environment) {
        super(cloudsim);
        Preconditions.checkArgument(environment.isHomogeneous(), "Expected environment to be homogeneous.");
        this.environment = environment;
    }

    public VMType getVmType() {
        return this.environment.getVmTypes().iterator().next();
    }
}
