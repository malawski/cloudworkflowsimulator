package cws.core.pricing;

import cws.core.exception.IllegalCWSArgumentException;
import cws.core.pricing.models.GooglePricingModel;
import cws.core.pricing.models.PricingModel;
import cws.core.pricing.models.SimplePricingModel;
import org.omg.CORBA.PRIVATE_MEMBER;

import java.util.Map;

/**
 * Created by Marcin Ziaber on 2016-10-11.
 */
public class PricingModelFactory {

    public static final String SIMPLE_MODEL = "simple";
    public static final String GOOGLE_MODEL = "google";

    public static PricingModel getPricingModel(Map<String, Object> pricingConfig) {
        assertOptionIsPresent(pricingConfig, PricingConfigLoader.MODEL_ENTRY);
        assertIsString(pricingConfig);
        assertOptionIsPresent(pricingConfig, PricingConfigLoader.BILLING_TIME_ENTRY);
        assertIsNumber(pricingConfig, PricingConfigLoader.BILLING_TIME_ENTRY);
        double billingTime = toDouble(pricingConfig, PricingConfigLoader.BILLING_TIME_ENTRY);
        assertIsGreaterThanZero(billingTime, PricingConfigLoader.BILLING_TIME_ENTRY);

        final String model = (String) pricingConfig.get(PricingConfigLoader.MODEL_ENTRY);

        final PricingModel pricingModel;

        if (model.equals(SIMPLE_MODEL)) {
            pricingModel = new SimplePricingModel(billingTime);
        } else if (model.equals(GOOGLE_MODEL)) {
            assertOptionIsPresent(pricingConfig, PricingConfigLoader.FIRST_BILLING_TIME_ENTRY);
            assertIsNumber(pricingConfig, PricingConfigLoader.FIRST_BILLING_TIME_ENTRY);
            double firstBillingTime = toDouble(pricingConfig, PricingConfigLoader.FIRST_BILLING_TIME_ENTRY);
            assertIsGreaterThanZero(firstBillingTime, PricingConfigLoader.FIRST_BILLING_TIME_ENTRY);
            pricingModel = new GooglePricingModel(billingTime, firstBillingTime);
        } else {
            throw new IllegalCWSArgumentException(
                    PricingConfigLoader.MODEL_ENTRY + " configuration is not a valid model name.");
        }

        return pricingModel;
    }

    private static void assertOptionIsPresent(Map<String, Object> pricingConfig, String configEntry) {
        if (!pricingConfig.containsKey(configEntry)) {
            throw new IllegalCWSArgumentException(configEntry + " configuration is missing.");
        }
    }

    private static void assertIsString(Map<String, Object> pricingConfig) {
        Object model = pricingConfig.get(PricingConfigLoader.MODEL_ENTRY);
        if (!(model instanceof String)) {
            throw new IllegalCWSArgumentException(PricingConfigLoader.MODEL_ENTRY + " configuration is not a string.");
        }
    }

    private static void assertIsNumber(Map<String, Object> config, String configEntry) {
        if (!(config.get(configEntry) instanceof Number)) {
            throw new IllegalCWSArgumentException(configEntry + " configuration is not a number");
        }
    }

    private static void assertIsGreaterThanZero(double value, String configEntry) {
        if (value <= 0) {
            throw new IllegalCWSArgumentException(configEntry + " configuration is not greater than zero");
        }
    }

    private static double toDouble(Map<String, Object> config, String configEntry) {
        Number value = (Number) config.get(configEntry);
        return value.doubleValue();
    }
}
