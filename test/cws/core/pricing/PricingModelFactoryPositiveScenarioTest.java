package cws.core.pricing;

import com.google.common.collect.ImmutableMap;
import cws.core.pricing.models.GooglePricingModel;
import cws.core.pricing.models.PricingModel;
import cws.core.pricing.models.SimplePricingModel;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import static org.junit.Assert.assertTrue;

/**
 * Created by Marcin Ziaber on 2016-10-24.
 */
@RunWith(Parameterized.class)
public class PricingModelFactoryPositiveScenarioTest {
    private Map<String, Object> pricingConfig;
    private PricingModel pricingModel;
    private static final PricingModelFactory pricingModelFactory = new PricingModelFactory();

    public PricingModelFactoryPositiveScenarioTest(Map<String, Object> pricingConfig, PricingModel pricingModel) {
        this.pricingConfig = pricingConfig;
        this.pricingModel = pricingModel;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {ImmutableMap.of(PricingConfigLoader.MODEL_ENTRY, "simple", PricingConfigLoader.BILLING_TIME_ENTRY, 60), new SimplePricingModel(60)},
                {ImmutableMap.of(PricingConfigLoader.MODEL_ENTRY, "google", PricingConfigLoader.BILLING_TIME_ENTRY, 60, PricingConfigLoader.FIRST_BILLING_TIME_ENTRY, 600), new GooglePricingModel(60, 600)}
        });
    }

    @Test
    public void getPricingModelPositiveScenarioTest() throws Exception {
        assertTrue(pricingModel.equals(pricingModelFactory.getPricingModel(pricingConfig)));
    }
}
