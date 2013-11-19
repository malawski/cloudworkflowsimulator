package cws.core.provisioner;

import java.util.Iterator;
import java.util.Set;

import cws.core.Provisioner;
import cws.core.VM;
import cws.core.WorkflowEngine;
import cws.core.WorkflowEvent;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.core.VMType;
import cws.core.core.VMTypeFactory;

public class SimpleQueueBasedProvisioner extends CloudAwareProvisioner implements Provisioner {

    public SimpleQueueBasedProvisioner(CloudSimWrapper cloudsim) {
        super(cloudsim);
    }

    @Override
    public void provisionResources(WorkflowEngine engine) {
        // use the queued (released) jobs from the workflow engine
        int queueLength = engine.getQueueLength();

        getCloudsim().log("Provisioner: queue length: " + queueLength);

        // check the deadline and budget constraints
        double budget = engine.getBudget();
        double deadline = engine.getDeadline();
        double time = getCloudsim().clock();
        double cost = engine.getCost();

        // if we are close to the budget by one VM*hour
        if (budget <= cost || time > deadline) {
            return;
        }

        // add one VM if queue not empty
        if (queueLength > 0) {
            VMType vmType = VMTypeFactory.getDefaults();
            VM vm = VMFactory.createVM(vmType, getCloudsim());

            getCloudsim().log("Starting VM: " + vm.getId());
            getCloudsim().send(engine.getId(), getCloud().getId(), 0.0, WorkflowEvent.VM_LAUNCH, vm);
        } else { // terminate free VMs
            Set<VM> freeVMs = engine.getFreeVMs();
            Iterator<VM> vmIt = freeVMs.iterator();
            while (vmIt.hasNext()) {
                VM vm = vmIt.next();
                vmIt.remove();
                getCloudsim().log("Terminating VM: " + vm.getId());
                getCloudsim().send(engine.getId(), getCloud().getId(), 0.0, WorkflowEvent.VM_TERMINATE, vm);
            }
        }
        getCloudsim().send(engine.getId(), engine.getId(), PROVISIONER_INTERVAL, WorkflowEvent.PROVISIONING_REQUEST,
                null);
    }
}
