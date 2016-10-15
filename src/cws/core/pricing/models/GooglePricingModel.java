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
        return 0;
    }

    @Override
    public double getRuntimeVmCost(double priceForBillingUnit, double runtimeInSeconds) {
        return 0;
    }

    @Override
    public double getAlreadyPaidCost(double priceForBillingUnit, double runtimeInSeconds) {
        return 0;
    }

    @Override
    public double getAllVMsCost(List<VM> vms) {
        return 0;
    }
}
