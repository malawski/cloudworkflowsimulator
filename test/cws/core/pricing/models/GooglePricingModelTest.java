package cws.core.pricing.models;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

/**
 * Created by Marcin Ziaber on 2016-10-23.
 */
public class GooglePricingModelTest {
    private final static double billingTimeInSeconds = 60;
    private final static double priceForBillingUnit = 1;
    private final static double firstBillingTimeInSeconds = 600;
    private final static double priceForFirstBillingUnit = priceForBillingUnit * firstBillingTimeInSeconds
            / billingTimeInSeconds;
    private final static double delta = 0.0001;

    private final GooglePricingModel googlePricingModel = new GooglePricingModel(billingTimeInSeconds,
            firstBillingTimeInSeconds);

    @Test
    public void testGetVmCostFor() throws Exception {
        assertEquals(priceForFirstBillingUnit, googlePricingModel.getVmCostFor(priceForBillingUnit, 0), delta);
        assertEquals(priceForFirstBillingUnit, googlePricingModel.getVmCostFor(priceForBillingUnit, 120), delta);
        assertEquals(priceForFirstBillingUnit + priceForBillingUnit,
                googlePricingModel.getVmCostFor(priceForBillingUnit, 610), delta);
    }

    @Test
    public void testGetRuntimeVmCost() throws Exception {
        assertEquals(0, googlePricingModel.getRuntimeVmCost(priceForBillingUnit, 0), delta);
        assertEquals(priceForFirstBillingUnit, googlePricingModel.getRuntimeVmCost(priceForBillingUnit, 120), delta);
        assertEquals(priceForFirstBillingUnit + priceForBillingUnit,
                googlePricingModel.getRuntimeVmCost(priceForBillingUnit, 620), delta);
    }

    @Test
    public void testGetAlreadyPaidCost() throws Exception {
        assertEquals(2., googlePricingModel.getAlreadyPaidCost(priceForBillingUnit, 120), delta);
        assertEquals(110 * priceForBillingUnit / billingTimeInSeconds,
                googlePricingModel.getAlreadyPaidCost(priceForBillingUnit, 110), delta);
        assertEquals(0., googlePricingModel.getAlreadyPaidCost(priceForBillingUnit, 0), delta);
    }

    @Test
    public void testGetRuntimeBasedOnBillingTime() throws Exception {
        assertEquals(0, googlePricingModel.getRuntimeBasedOnBillingTime(0), delta);
        assertEquals(firstBillingTimeInSeconds, googlePricingModel.getRuntimeBasedOnBillingTime(120), delta);
        assertEquals(firstBillingTimeInSeconds + 60, googlePricingModel.getRuntimeBasedOnBillingTime(630), delta);
    }

    @Test
    public void testGetFullRuntime() throws Exception {
        assertEquals(firstBillingTimeInSeconds, googlePricingModel.getFullRuntime(0, 0), delta);
        assertEquals(firstBillingTimeInSeconds, googlePricingModel.getFullRuntime(0, 120), delta);
        assertEquals(firstBillingTimeInSeconds + 60, googlePricingModel.getFullRuntime(0, 630), delta);
    }

    @Test
    public void testPriceForFirstBillingUnit() {
        assertEquals(priceForBillingUnit * firstBillingTimeInSeconds / billingTimeInSeconds,
                googlePricingModel.getPriceForFirstBillingUnit(priceForBillingUnit), delta);
    }

}
