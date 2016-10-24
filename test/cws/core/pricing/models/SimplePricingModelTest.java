package cws.core.pricing.models;

import junit.framework.TestCase;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by Marcin Ziaber on 2016-10-23.
 */
public class SimplePricingModelTest {

    private final static double billingTimeInSeconds = 60;
    private final static double priceForBillingUnit = 1;

    private final SimplePricingModel simplePricingModel = new SimplePricingModel(billingTimeInSeconds);


    @Test
    public void testGetVMCost() {
        assertTrue(priceForBillingUnit == simplePricingModel.getVmCostFor(priceForBillingUnit, 0));
        assertTrue(priceForBillingUnit * 2 == simplePricingModel.getVmCostFor(priceForBillingUnit, 120));
    }

    @Test
    public void testGetRuntimeVMCost() {
        assertTrue(0. == simplePricingModel.getRuntimeVmCost(priceForBillingUnit, 0));
        assertTrue(priceForBillingUnit * 2 == simplePricingModel.getRuntimeVmCost(priceForBillingUnit, 120));
    }

    @Test
    public void testGetAlreadyPaidCost() {
        assertTrue(2. == simplePricingModel.getAlreadyPaidCost(priceForBillingUnit, 120));
        assertTrue(110 * priceForBillingUnit / billingTimeInSeconds == simplePricingModel.getAlreadyPaidCost(priceForBillingUnit, 110));
        assertTrue(0. == simplePricingModel.getAlreadyPaidCost(priceForBillingUnit, 0));
    }

    @Test
    public void testGetRuntimeBasedOnBillingTime() {
        assertTrue(120. == simplePricingModel.getRuntimeBasedOnBillingTime(110));
    }

    @Test
    public void testGetFullRuntime() {
        assertTrue(120. == simplePricingModel.getFullRuntime(0, 110));
        assertTrue(billingTimeInSeconds == simplePricingModel.getFullRuntime(0, 50));
    }
}