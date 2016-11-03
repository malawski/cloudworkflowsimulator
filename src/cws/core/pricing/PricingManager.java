package cws.core.pricing;

import java.util.List;

import cws.core.VM;
import cws.core.core.VMType;
import cws.core.pricing.models.PricingModel;

/**
 * Created by Marcin Ziaber on 2016-10-09.
 */
public class PricingManager {

    private PricingModel pricingModel;

    public PricingManager(PricingModel pricingModel) {
        this.pricingModel = pricingModel;
    }

    /**
     * Compute the total cost of VM. This is computed by taking the
     * runtime, rounding it up to the nearest whole billing unit, and multiplying
     * by the billing unit price (also adding price for firstBillingUnitTime when pricing model is set to Google)
     * It can return 0 if runtime = 0.
     */
    public double getRuntimeVMCost(VM vm) {
        final double priceForBillingUnit = vm.getVmType().getPriceForBillingUnit();
        return this.pricingModel.getRuntimeVmCost(priceForBillingUnit, vm.getRuntime());
    }

    /**
     * Same as in {@link cws.core.pricing.PricingManager#getRuntimeVMCost(VM)}, besides it can not return 0. Cost is
     * always
     * at least cost of first billing unit.
     */
    public double getVMCostFor(VMType vmType, double runtimeInSeconds) {
        final double priceForBillingUnit = vmType.getPriceForBillingUnit();
        return this.pricingModel.getVmCostFor(priceForBillingUnit, runtimeInSeconds);
    }

    /**
     * Compute cost of all VMs by summing up runtime cost (using
     * {@link cws.core.pricing.PricingManager#getRuntimeVMCost(VM)}).
     */
    public double getAllVMsCost(List<VM> vms) {
        return this.pricingModel.getAllVMsCost(vms);
    }

    /**
     * Compute already paid cost without ceiling it up to full billing unit.
     * This is required for admissioning (see {@link cws.core.scheduler.RuntimeWorkflowAdmissioner})
     */
    public double getAlreadyPaidCost(VM vm) {
        final double priceForBillingUnit = vm.getVmType().getPriceForBillingUnit();
        return this.pricingModel.getAlreadyPaidCost(priceForBillingUnit, vm.getRuntime());
    }

    /**
     * Compute runtime rounded up to the nearest billing unit.
     */
    public double getFullRuntime(double runtime) {
        return pricingModel.getRuntimeBasedOnBillingTime(runtime);
    }

    /**
     * Compute runtime rounded up to the nearest billing unit.
     * It can not return 0. At least first billing unit will be counted.
     */
    public double getFullRuntime(double start, double end) {
        return pricingModel.getFullRuntime(start, end);
    }

    public double getBillingTimeInSeconds() {
        return pricingModel.getBillingTimeInSeconds();
    }

    public double getPriceForFirstBillingUnit(double vmPrice) {
        return pricingModel.getPriceForFirstBillingUnit(vmPrice);
    }

    @Override
    public String toString() {
        return "PricingManager{" + "pricingModel=" + pricingModel + '}';
    }
}
