package cws.core.pricing.models;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by Marcin Ziaber on 2016-10-23.
 */
public class SimplePricingModelTest {

    private final static double billingTimeInSeconds = 60;
    private final static double priceForBillingUnit = 1;
    private final static double delta = 0.0001;

    private final SimplePricingModel simplePricingModel = new SimplePricingModel(billingTimeInSeconds);

    @Test
    public void testGetVMCost() {
        assertEquals(priceForBillingUnit, simplePricingModel.getVmCostFor(priceForBillingUnit, 0), delta);
        assertEquals(priceForBillingUnit * 2, simplePricingModel.getVmCostFor(priceForBillingUnit, 120), delta);
    }

    @Test
    public void testGetRuntimeVMCost() {
        assertEquals(0., simplePricingModel.getRuntimeVmCost(priceForBillingUnit, 0), delta);
        assertEquals(priceForBillingUnit * 2, simplePricingModel.getRuntimeVmCost(priceForBillingUnit, 120), delta);
    }

    @Test
    public void testGetAlreadyPaidCost() {
        assertEquals(2., simplePricingModel.getAlreadyPaidCost(priceForBillingUnit, 120), delta);
        assertEquals(110 * priceForBillingUnit / billingTimeInSeconds,
                simplePricingModel.getAlreadyPaidCost(priceForBillingUnit, 110), delta);
        assertEquals(0., simplePricingModel.getAlreadyPaidCost(priceForBillingUnit, 0), delta);
    }

    @Test
    public void testGetRuntimeBasedOnBillingTime() {
        assertEquals(120., simplePricingModel.getRuntimeBasedOnBillingTime(110), delta);
    }

    @Test
    public void testGetFullRuntime() {
        assertEquals(120., simplePricingModel.getFullRuntime(0, 110), delta);
        assertEquals(billingTimeInSeconds, simplePricingModel.getFullRuntime(0, 50), delta);
    }
}
