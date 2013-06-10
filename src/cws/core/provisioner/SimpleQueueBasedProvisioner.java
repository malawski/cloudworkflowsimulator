package cws.core.provisioner;

import java.util.Iterator;
import java.util.Set;

import cws.core.*;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.experiment.VMFactory;

public class SimpleQueueBasedProvisioner extends AbstractProvisioner implements Provisioner {

    public SimpleQueueBasedProvisioner(CloudSimWrapper cloudsim) {
        super(cloudsim);
    }

    @Override
    public void provisionResources(WorkflowEngine engine) {

        // use the queued (released) jobs from the workflow engine
        int queueLength = engine.getQueueLength();

        getCloudSim().log(" Provisioner: queue length: " + queueLength);

        // check the deadline and budget constraints
        double budget = engine.getBudget();
        double deadline = engine.getDeadline();
        double time = getCloudSim().clock();
        double cost = engine.getCost();

        // if we are close to the budget by one VM*hour
        if (budget <= cost || time > deadline) {
            return;
        }

        // add one VM if queue not empty
        if (queueLength > 0) {
            VMStaticParams vmStaticParams = VMStaticParams.getDefaults();
            VM vm = VMFactory.createVM(vmStaticParams, getCloudSim());

            getCloudSim().log(" Starting VM: " + vm.getId());
            getCloudSim().send(engine.getId(), cloud.getId(), 0.0, WorkflowEvent.VM_LAUNCH, vm);
        } else { // terminate free VMs
            Set<VM> freeVMs = engine.getFreeVMs();
            Iterator<VM> vmIt = freeVMs.iterator();
            while (vmIt.hasNext()) {
                VM vm = vmIt.next();
                vmIt.remove();
                getCloudSim().log(" Terminating VM: " + vm.getId());
                getCloudSim().send(engine.getId(), cloud.getId(), 0.0, WorkflowEvent.VM_TERMINATE, vm);
            }
        }
        getCloudSim().send(engine.getId(), engine.getId(), PROVISIONER_INTERVAL, WorkflowEvent.PROVISIONING_REQUEST,
                null);
    }
}
