package cws.core.provisioner;

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
    private VMType vmType;

    public HomogeneousProvisioner (CloudSimWrapper cloudsim, VMType vmType) {
        super(cloudsim);
        this.vmType = vmType;
    }

    public void setEnvironment(Environment environment) {
        this.environment = environment;
    }

    public VMType getVmType() {
        return vmType;
    }
}
