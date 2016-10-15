package cws.core.pricing;

import cws.core.VM;
import cws.core.core.VMType;
import cws.core.pricing.models.PricingModel;

import java.util.List;

/**
 * Created by Marcin Ziaber on 2016-10-09.
 */
public class PricingManager {

    private PricingModel pricingModel;

    public PricingManager(PricingModel pricingModel) {
        this.pricingModel = pricingModel;
    }

    public double getVMCostFor(VMType vmType, double runtimeInSeconds) {
        final double priceForBillingUnit = vmType.getPriceForBillingUnit();
        return this.pricingModel.getVmCostFor(priceForBillingUnit, runtimeInSeconds);
    }

    /**
     * Compute the total cost of VM. This is computed by taking the
     * runtime, rounding it up to the nearest whole billing unit, and multiplying
     * by the billing unit price.
     */
    public double getRuntimeVMCost(VM vm){
        final double priceForBillingUnit = vm.getVmType().getPriceForBillingUnit();
        return this.pricingModel.getRuntimeVmCost(priceForBillingUnit, vm.getRuntime());
    }

    public double getAllVMsCost(List<VM> vms){
        return this.pricingModel.getAllVMsCost(vms);
    }

    public double getAlreadyPaidCost(VM vm){
        final double priceForBillingUnit = vm.getVmType().getPriceForBillingUnit();
        return this.pricingModel.getAlreadyPaidCost(priceForBillingUnit, vm.getRuntime());
    }
    @Override
    public String toString() {
        return "PricingManager{" +
                "pricingModel=" + pricingModel +
                '}';
    }
    public double getFullRuntime(double runtime) {
        return pricingModel.getRuntimeBasedOnBillingTime(runtime);
    }
    public double getFullRuntime(double start, double end){
        return pricingModel.getFullRuntime(start, end);
    }
    public double getBillingTimeInSeconds() {
        return pricingModel.getBillingTimeInSeconds();
    }
}
