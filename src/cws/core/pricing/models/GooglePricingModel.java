package cws.core.pricing.models;

import cws.core.VM;

import java.util.List;

/**
 * Created by Marcin Ziaber on 2016-10-09.
 * <p>
 * Pricing model used by Google. First we pay in advance for defined long period of time,
 * and then we switch to the simple model of paying periodically for shorter period of time.
 */
public class GooglePricingModel extends PricingModel {
    /**
     * For how long we pay in advance for first time
     */
    private final double firstBillingTimeInSeconds;


    public GooglePricingModel(double billingTimeInSeconds, double firstBillingTimeInSeconds) {
        super(billingTimeInSeconds);
        this.firstBillingTimeInSeconds = firstBillingTimeInSeconds;
    }

    @Override
    public String toString() {
        return "GooglePricingModel billingTime: " + billingTimeInSeconds + ", firstBillingTime: " + firstBillingTimeInSeconds;
    }

    @Override
    public double getVmCostFor(double priceForBillingUnit, double runtimeInSeconds) {
        double totalVMCost = 0;
        totalVMCost += firstBillingTimeInSeconds * priceForBillingUnit / billingTimeInSeconds; //assuming that firstBillingTimeInSeconds is multiply of billingTimeInSeconds
        if(runtimeInSeconds > firstBillingTimeInSeconds){
            runtimeInSeconds -= firstBillingTimeInSeconds;
            double billingUnits = runtimeInSeconds / billingTimeInSeconds;
            int fullBillingUnits = (int) Math.ceil(billingUnits);
            totalVMCost += Math.max(1, fullBillingUnits) * priceForBillingUnit;
        }

        return totalVMCost;
    }

    @Override
    public double getRuntimeVmCost(double priceForBillingUnit, double runtimeInSeconds) {
        double totalVMCost = 0;
        if(runtimeInSeconds > 0){
            totalVMCost += firstBillingTimeInSeconds * priceForBillingUnit / billingTimeInSeconds;
        }
        if(runtimeInSeconds>firstBillingTimeInSeconds){
            runtimeInSeconds -= firstBillingTimeInSeconds;
            double billingUnits = runtimeInSeconds / billingTimeInSeconds;
            double fullBillingUnits = Math.ceil(billingUnits);
            totalVMCost += fullBillingUnits * priceForBillingUnit;
        }

        return totalVMCost;
    }

    @Override
    public double getAlreadyPaidCost(double priceForBillingUnit, double runtimeInSeconds) {
        double totalVMCost = 0;
        totalVMCost += firstBillingTimeInSeconds * priceForBillingUnit / billingTimeInSeconds;
        if(runtimeInSeconds > firstBillingTimeInSeconds){
            runtimeInSeconds -= firstBillingTimeInSeconds;
            totalVMCost = runtimeInSeconds * priceForBillingUnit / billingTimeInSeconds;
        }
        return  totalVMCost;
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
        runtime -= firstBillingTimeInSeconds;
        if(runtime<=0) return firstBillingTimeInSeconds;
        else{
            int runtimeUnits = (int) Math.ceil(runtime / billingTimeInSeconds);
            return (runtimeUnits * billingTimeInSeconds) + firstBillingTimeInSeconds;
        }
    }

    @Override
    public double getFullRuntime(double start, double end) {
        double runtime = (end - start) - firstBillingTimeInSeconds;
        if(runtime<=0) return firstBillingTimeInSeconds;
        else {
            double units = runtime / billingTimeInSeconds;
            int rounded = (int) Math.ceil(units);
            return Math.max(1, rounded) * billingTimeInSeconds + firstBillingTimeInSeconds;
        }
    }
}
