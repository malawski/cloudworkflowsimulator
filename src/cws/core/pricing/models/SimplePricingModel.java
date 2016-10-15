package cws.core.pricing.models;

import cws.core.VM;
import cws.core.core.VMType;

import java.util.List;

import static java.lang.Runtime.getRuntime;

/**
 * Created by Marcin Ziaber on 2016-10-09.
 * <p>
 * Pricing model in which we pay periodically in advance for defined in config period of time.
 */
public class SimplePricingModel extends PricingModel {

    public SimplePricingModel(double billingTimeInSeconds) {
        super(billingTimeInSeconds);
    }

    @Override
    public double getVmCostFor(double priceForBillingUnit, double runtimeInSeconds) {
        double billingUnits = runtimeInSeconds / billingTimeInSeconds;
        int fullBillingUnits = (int) Math.ceil(billingUnits);
        return Math.max(1, fullBillingUnits) * priceForBillingUnit;
    }

    @Override
    public double getRuntimeVmCost(double priceForBillingUnit, double runtimeInSeconds) {
        double billingUnits = runtimeInSeconds / billingTimeInSeconds;
        double fullBillingUnits = Math.ceil(billingUnits);
        return fullBillingUnits * priceForBillingUnit;
    }

    @Override
    public double getAlreadyPaidCost(double priceForBillingUnit, double runtimeInSeconds) {
        return runtimeInSeconds * priceForBillingUnit / billingTimeInSeconds;
    }

    @Override
    public double getAllVMsCost(List<VM> vms) {
        double cost = 0;
        for (VM vm : vms) {
            cost += this.getRuntimeVmCost(vm.getVmType().getPriceForBillingUnit(), vm.getRuntime());
        }
        return cost;
    }

    @Override
    public double getRuntimeBasedOnBillingTime(double runtime) {
        int runtimeUnits = (int) Math.ceil(runtime / billingTimeInSeconds);
        return (runtimeUnits * billingTimeInSeconds);
    }

    @Override
    public double getFullRuntime(double start, double end) {
        double seconds = end - start;
        double units = seconds / billingTimeInSeconds;
        int rounded = (int) Math.ceil(units);
        return Math.max(1, rounded) * billingTimeInSeconds;
    }


    @Override
    public String toString() {
        return "SimplePricingModel billingTime:" + billingTimeInSeconds;
    }
}