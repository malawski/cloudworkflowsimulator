package cws.core.pricing.models;

import cws.core.VM;

import java.util.List;

import static org.junit.Assert.assertTrue;

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
        double totalVMCost = firstBillingTimeInSeconds * priceForBillingUnit / billingTimeInSeconds; //assuming that firstBillingTimeInSeconds is multiply of billingTimeInSeconds
        if (runtimeInSeconds > firstBillingTimeInSeconds) {
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
        if (runtimeInSeconds > 0) {
            totalVMCost += firstBillingTimeInSeconds * priceForBillingUnit / billingTimeInSeconds;
        }
        if (runtimeInSeconds > firstBillingTimeInSeconds) {
            runtimeInSeconds -= firstBillingTimeInSeconds;
            double billingUnits = runtimeInSeconds / billingTimeInSeconds;
            double fullBillingUnits = Math.ceil(billingUnits);
            totalVMCost += fullBillingUnits * priceForBillingUnit;
        }

        return totalVMCost;
    }

    @Override
    public double getAlreadyPaidCost(double priceForBillingUnit, double runtimeInSeconds) {
        return runtimeInSeconds * priceForBillingUnit / billingTimeInSeconds;
    }

    @Override
    public double getRuntimeBasedOnBillingTime(double runtime) {
        double runtimeBasedOnBillingTime = 0;
        if (runtime > 0) {
            runtimeBasedOnBillingTime += firstBillingTimeInSeconds;
        }
        if (runtime > firstBillingTimeInSeconds) {
            runtime-=firstBillingTimeInSeconds;
            int runtimeUnits = (int) Math.ceil(runtime / billingTimeInSeconds);
            runtimeBasedOnBillingTime += (runtimeUnits * billingTimeInSeconds);
        }
        return runtimeBasedOnBillingTime;
    }

    @Override
    public double getFullRuntime(double start, double end) {
        double runtime = (end - start) - firstBillingTimeInSeconds;
        if (runtime <= 0) return firstBillingTimeInSeconds;
        else {
            double units = runtime / billingTimeInSeconds;
            int rounded = (int) Math.ceil(units);
            return Math.max(1, rounded) * billingTimeInSeconds + firstBillingTimeInSeconds;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        GooglePricingModel that = (GooglePricingModel) o;

        return Double.compare(that.firstBillingTimeInSeconds, firstBillingTimeInSeconds) == 0 &&
                Double.compare(that.billingTimeInSeconds, billingTimeInSeconds) == 0;

    }

    @Override
    public int hashCode() {
        long temp = Double.doubleToLongBits(firstBillingTimeInSeconds) + Double.doubleToLongBits(billingTimeInSeconds);
        return (int) (temp ^ (temp >>> 32));
    }
}
