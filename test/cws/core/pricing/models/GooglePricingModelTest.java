package cws.core.pricing.models;


import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * Created by Marcin Ziaber on 2016-10-23.
 */
public class GooglePricingModelTest {
    private final static double billingTimeInSeconds = 60;
    private final static double priceForBillingUnit = 1;
    private final static double firstBillingTimeInSeconds = 600;
    private final static double priceForFirstBillingUnit = priceForBillingUnit*firstBillingTimeInSeconds/billingTimeInSeconds;

    private final GooglePricingModel googlePricingModel = new GooglePricingModel(billingTimeInSeconds, firstBillingTimeInSeconds);
    @Test
    public void testGetVmCostFor() throws Exception {
        assertTrue(priceForFirstBillingUnit == googlePricingModel.getVmCostFor(priceForBillingUnit, 0));
        assertTrue(priceForFirstBillingUnit == googlePricingModel.getVmCostFor(priceForBillingUnit, 120));
        assertTrue(priceForFirstBillingUnit+priceForBillingUnit == googlePricingModel.getVmCostFor(priceForBillingUnit, 610));
    }

    public void testGetRuntimeVmCost() throws Exception {
        assertTrue(0 == googlePricingModel.getRuntimeVmCost(priceForBillingUnit, 0));
        assertTrue(priceForFirstBillingUnit == googlePricingModel.getRuntimeVmCost(priceForBillingUnit, 120));
        assertTrue(priceForFirstBillingUnit+priceForBillingUnit == googlePricingModel.getRuntimeVmCost(priceForBillingUnit, 620));
    }

    public void testGetAlreadyPaidCost() throws Exception {
        assertTrue(2. == googlePricingModel.getAlreadyPaidCost(priceForBillingUnit, 120));
        assertTrue(110 * priceForBillingUnit / billingTimeInSeconds == googlePricingModel.getAlreadyPaidCost(priceForBillingUnit, 110));
        assertTrue(0. == googlePricingModel.getAlreadyPaidCost(priceForBillingUnit, 0));
    }

    public void testGetRuntimeBasedOnBillingTime() throws Exception {
        assertTrue(0 == googlePricingModel.getRuntimeBasedOnBillingTime(0));
        assertTrue(firstBillingTimeInSeconds == googlePricingModel.getRuntimeBasedOnBillingTime(120));
        assertTrue(firstBillingTimeInSeconds+60 == googlePricingModel.getRuntimeBasedOnBillingTime(130));
    }

    public void testGetFullRuntime() throws Exception {
        assertTrue(firstBillingTimeInSeconds == googlePricingModel.getFullRuntime(0,0));
        assertTrue(firstBillingTimeInSeconds == googlePricingModel.getFullRuntime(0,120));
        assertTrue(firstBillingTimeInSeconds+60 == googlePricingModel.getFullRuntime(0, 130));
    }

}