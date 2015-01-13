package cws.core;

import cws.core.Cloud;
import cws.core.VM;
import cws.core.cloudsim.CWSSimEntity;
import cws.core.cloudsim.CloudSimWrapper;

/**  An interface for resource provisioners used by the WorkflowEngine.
 */
public abstract class Provisioner extends CWSSimEntity {

    private Cloud cloud;

    public Provisioner(CloudSimWrapper cloudsim) {
        super("Provisioner", cloudsim);
    }

    public void setCloud(Cloud cloud) {
        this.cloud = cloud;
    }

    public Cloud getCloud() {
        return cloud;
    }

    public void launchVM(VM vm) {
        cloud.launchVM(getId(), vm);
    }

    public void launchVMAtTime(VM vm, double launchTime) {
        cloud.launchVMAtTime(getId(), vm, launchTime);
    }

    public void terminateVM(VM vm) {
        cloud.terminateVM(vm);
    }


    public abstract void provisionResources(WorkflowEngine engine);
}
