package cws.core.provisioner;

import cws.core.WorkflowEngine;
import cws.core.cloudsim.CloudSimWrapper;


/** A provisioner which does nothing when provisionResources() is called.
 * Mainly for use with StaticAlgorithm: provides the algorithm with the
 * ability to create VMs but does not create any itself.
 */
public class NullProvisioner extends CloudAwareProvisioner  {

    public NullProvisioner(CloudSimWrapper cloudsim) {
        super(cloudsim);
    }

    public void provisionResources(WorkflowEngine engine) {}
}
