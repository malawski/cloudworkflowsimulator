package cws.core.provisioner;

import com.google.common.base.Preconditions;
import cws.core.Provisioner;
import cws.core.VM;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.core.VMType;
import cws.core.engine.Environment;

/** A provisioner base class for when only a single type of VM and single
 * StorageManager is in use (and so an Environment class can provide all
 * the information needed).
 */
public abstract class HomogeneousProvisioner extends Provisioner {

    protected Environment environment;
    private VMType vmType;

    public HomogeneousProvisioner (CloudSimWrapper cloudsim) {
        super(cloudsim);
    }

    public void setEnvironment(Environment environment) {
        Preconditions.checkArgument(environment.isHomogeneous(), "Expected environment to be homogeneous.");
        this.environment = environment;
        this.vmType = this.environment.getVmTypes().iterator().next();
    }

    public VMType getVmType() {
        return this.vmType;
    }
}
