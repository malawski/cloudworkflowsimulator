package cws.core.provisioner;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import cws.core.Provisioner;
import cws.core.VM;
import cws.core.WorkflowEngine;
import cws.core.WorkflowEvent;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.experiment.VMFactory;

public class SimpleUtilizationBasedProvisioner extends AbstractProvisioner implements Provisioner {

    // above this utilization threshold we start provisioning additional VMs
    private static final double UPPER_THRESHOLD = 0.90;
    // below this utilization threshold we start deprovisioning vms
    private static final double LOWER_THRESHOLD = 0.70;

    // conservative estimate of deprovisioning delay
    // private static final double DEPROVISIONING_DELAY_ESTIMATE = 25.0;
    // optimistic estimate of deprovisioning delay
    private static final double DEPROVISIONING_DELAY_ESTIMATE = 1.0;

    // number of initially provisioned VMs to be used for setting limits for autoscaling
    private int initialNumVMs = 0;

    public SimpleUtilizationBasedProvisioner(double maxScaling, CloudSimWrapper cloudsim) {
        super(maxScaling, cloudsim);
        // TODO Auto-generated constructor stub
    }

    @Override
    public void provisionResources(WorkflowEngine engine) {

        // when called for the first time it should obtain the initial number of VMs
        if (initialNumVMs == 0)
            initialNumVMs = engine.getAvailableVMs().size();

        // check the deadline and budget constraints

        double budget = engine.getBudget();
        double deadline = engine.getDeadline();
        double time = getCloudSim().clock();
        double cost = engine.getCost();

        getCloudSim().log(" Provisioner: Budget consumed " + cost);

        // assuming all VMs are homogeneous
        double vmPrice = 0;
        if (!engine.getAvailableVMs().isEmpty())
            vmPrice = engine.getAvailableVMs().get(0).getPrice();

        // running vms are free + busy
        Set<VM> runningVMs = new HashSet<VM>(engine.getFreeVMs());
        runningVMs.addAll(engine.getBusyVMs());

        int numVMsRunning = runningVMs.size();

        // find VMs that will complete their billing hour
        // during the next provisioning cycle
        Set<VM> completingVMs = new HashSet<VM>();

        for (VM vm : runningVMs) {
            double vmRuntime = vm.getRuntime();

            // full hours (rounded up)
            double vmHours = Math.ceil(vmRuntime / 3600.0);

            // seconds till next full hour
            double secondsRemaining = vmHours * 3600.0 - vmRuntime;

            // we add delay estimate to include also the deprovisioning time
            if (secondsRemaining < PROVISIONER_INTERVAL + DEPROVISIONING_DELAY_ESTIMATE) {
                completingVMs.add(vm);
            }
        }

        int numVMsCompleting = completingVMs.size();

        getCloudSim().log(" Provisioner: number of instances : " + numVMsRunning);
        getCloudSim().log(" Provisioner: number of instances completing: " + numVMsCompleting);

        // if we are close to the budget
        if (budget - cost < vmPrice * numVMsCompleting || time > deadline) {

            // compute number of vms to terminate
            // it is the number that would overrun the budget if not terminated
            int numToTerminate = numVMsRunning - (int) Math.floor(((budget - cost) / vmPrice));

            // even if we have some budget left we should terminate all the instances past the deadline.
            if (time > deadline)
                numToTerminate = numVMsRunning;

            getCloudSim().log(" Provisioner: number of instances to terminate: " + numToTerminate);

            // set of vms scheduled for termination
            Set<VM> toTerminate = new HashSet<VM>();

            // select VMs to terminate
            if (numToTerminate < numVMsCompleting) {
                // select only from completing vms
                Iterator<VM> completingIt = completingVMs.iterator();
                for (int i = 0; i < numToTerminate; i++) {
                    VM vm;
                    vm = completingIt.next();
                    toTerminate.add(vm);
                }
            } else {
                // terminate all completing and add more from free and busy ones
                toTerminate.addAll(completingVMs);
                // int added = toTerminate.size();
                //
                // Iterator<VM> freeIt = engine.getFreeVMs().iterator();
                // Iterator<VM> busyIt = engine.getBusyVMs().iterator();
                // while (added<numToTerminate) {
                // VM vm;
                // if (freeIt.hasNext()) vm = freeIt.next();
                // else vm = busyIt.next();
                // if (toTerminate.add(vm)) added++;
                // }

            }

            // start terminating vms
            Set<VM> terminated = terminateInstances(engine, toTerminate);
            // remove terminated vms from free and busy sets
            engine.getFreeVMs().removeAll(terminated);
            engine.getBusyVMs().removeAll(terminated);

            // some instances may be still running so we want to be invoked again to stop them before they reach full
            // hour
            if (engine.getFreeVMs().size() + engine.getBusyVMs().size() > 0)
                getCloudSim().send(engine.getId(), engine.getId(), PROVISIONER_INTERVAL,
                        WorkflowEvent.PROVISIONING_REQUEST, null);
            // return without further provisioning
            return;
        }

        // compute utilization
        double numFreeVMS = engine.getFreeVMs().size();
        double numBusyVMs = engine.getBusyVMs().size();
        double utilization = numBusyVMs / (numFreeVMS + numBusyVMs);

        getCloudSim().log(" Provisioner: utilization: " + utilization);

        // if we are close to constraints we should not provision new vms
        boolean finishing_phase = budget - cost <= vmPrice * numVMsRunning || time > deadline;

        // if:
        // we are not in finishing phase,
        // and utilization is high
        // and we are below max limit
        // and we have money left for one instance more
        // then: deploy new instance
        if (!finishing_phase && utilization > UPPER_THRESHOLD
                && numBusyVMs + numFreeVMS < getMaxScaling() * initialNumVMs && budget - cost >= vmPrice) {

            VM vm = VMFactory.createVM(1000, 1, 1.0, 1.0, getCloudSim());
            getCloudSim().log(" Starting VM: " + vm.getId());
            getCloudSim().send(engine.getId(), cloud.getId(), 0.0, WorkflowEvent.VM_LAUNCH, vm);

        } else if (!finishing_phase && utilization < LOWER_THRESHOLD) {

            // select Vms to terminate
            Set<VM> toTerminate = new HashSet<VM>();

            // terminate half of the instances
            // make sure that if there is only one instance it should be terminated
            int numToTerminate = (int) Math.ceil(engine.getFreeVMs().size() / 2.0);
            Iterator<VM> vmIt = engine.getFreeVMs().iterator();
            for (int i = 0; i < numToTerminate && vmIt.hasNext(); i++) {
                toTerminate.add(vmIt.next());
            }

            Set<VM> terminated = terminateInstances(engine, toTerminate);
            engine.getFreeVMs().removeAll(terminated);

        }

        // send event to initiate next provisioning cycle
        getCloudSim().send(engine.getId(), engine.getId(), PROVISIONER_INTERVAL, WorkflowEvent.PROVISIONING_REQUEST,
                null);

    }

    /**
     * This method terminates instances but only the ones
     * that are close to the full hour of operation.
     * Thus this method has to be invoked several times
     * to effectively terminate all the instances.
     * The method modifies the given vmSet by removing the terminated Vms.
     * 
     * 
     * @param engine
     * @param vmSet
     * @return set of VMs that were terminated
     */

    private Set<VM> terminateInstances(WorkflowEngine engine, Set<VM> vmSet) {

        Set<VM> removed = new HashSet<VM>();
        Iterator<VM> vmIt = vmSet.iterator();

        while (vmIt.hasNext()) {
            VM vm = vmIt.next();
            double vmRuntime = vm.getRuntime();

            // full hours (rounded up)
            double vmHours = Math.ceil(vmRuntime / 3600.0);

            // seconds till next full hour
            double secondsRemaining = vmHours * 3600.0 - vmRuntime;

            // terminate only vms that have less seconds remaining than a defined threshold
            if (secondsRemaining < PROVISIONER_INTERVAL + DEPROVISIONING_DELAY_ESTIMATE) {
                vmIt.remove();
                removed.add(vm);
                getCloudSim().log(" Terminating VM: " + vm.getId());
                getCloudSim().send(engine.getId(), cloud.getId(), 0.0, WorkflowEvent.VM_TERMINATE, vm);
            }
        }
        return removed;
    }
}
