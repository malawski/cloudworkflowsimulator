package cws.core.provisioner;

import cws.core.Cloud;
import cws.core.Provisioner;
import cws.core.VM;
import cws.core.cloudsim.CWSSimEntity;
import cws.core.cloudsim.CloudSimWrapper;

public abstract class CloudAwareProvisioner extends CWSSimEntity implements Provisioner {

    private Cloud cloud;

    public CloudAwareProvisioner(CloudSimWrapper cloudsim) {
        super("CloudAwareProvisioner", cloudsim);
    }

    public void setCloud(Cloud cloud) {
        this.cloud = cloud;
    }

    public void launchVM(int id, VM vm) {
        cloud.launchVM(id, vm);
    }

    public void terminateVM(VM vm) {
        cloud.terminateVM(vm);
    }
}
