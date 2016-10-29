package cws.core.pricing;

import com.google.common.collect.ImmutableMap;
import cws.core.exception.IllegalCWSArgumentException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

/**
 * Created by Marcin Ziaber on 2016-10-24.
 */
@RunWith(Parameterized.class)
public class PricingModelFactoryNegativeScenarioTest {
    private Map<String, Object> pricingConfig;
    private String expectedException;

    private static final PricingModelFactory pricingModelFactory = new PricingModelFactory();

    public PricingModelFactoryNegativeScenarioTest(Map<String, Object> pricingConfig, String expectedException) {
        this.pricingConfig = pricingConfig;
        this.expectedException = expectedException;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                { ImmutableMap.of("test", "simple"), PricingConfigLoader.MODEL_ENTRY + " configuration is missing." },
                { ImmutableMap.of(PricingConfigLoader.MODEL_ENTRY, 123),
                        PricingConfigLoader.MODEL_ENTRY + " configuration is not a string." },
                { ImmutableMap.of(PricingConfigLoader.MODEL_ENTRY, "simple"),
                        PricingConfigLoader.BILLING_TIME_ENTRY + " configuration is missing." },
                { ImmutableMap.of(PricingConfigLoader.MODEL_ENTRY, "simple", PricingConfigLoader.BILLING_TIME_ENTRY,
                        "string"), PricingConfigLoader.BILLING_TIME_ENTRY + " configuration is not a number" },
                { ImmutableMap.of(PricingConfigLoader.MODEL_ENTRY, "simple", PricingConfigLoader.BILLING_TIME_ENTRY, 0),
                        PricingConfigLoader.BILLING_TIME_ENTRY + " configuration is not greater than zero" },
                { ImmutableMap.of(PricingConfigLoader.MODEL_ENTRY, "google", PricingConfigLoader.BILLING_TIME_ENTRY,
                        10), PricingConfigLoader.FIRST_BILLING_TIME_ENTRY + " configuration is missing." },
                { ImmutableMap.of(PricingConfigLoader.MODEL_ENTRY, "google", PricingConfigLoader.BILLING_TIME_ENTRY, 10,
                        PricingConfigLoader.FIRST_BILLING_TIME_ENTRY, "string"),
                        PricingConfigLoader.FIRST_BILLING_TIME_ENTRY + " configuration is not a number" },
                { ImmutableMap.of(PricingConfigLoader.MODEL_ENTRY, "google", PricingConfigLoader.BILLING_TIME_ENTRY, 10,
                        PricingConfigLoader.FIRST_BILLING_TIME_ENTRY, 0),
                        PricingConfigLoader.FIRST_BILLING_TIME_ENTRY + " configuration is not greater than zero" },
                { ImmutableMap.of(PricingConfigLoader.MODEL_ENTRY, "test", PricingConfigLoader.BILLING_TIME_ENTRY, 10,
                        PricingConfigLoader.FIRST_BILLING_TIME_ENTRY, 0),
                        PricingConfigLoader.MODEL_ENTRY + " configuration is not a valid model name." } });
    }

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void getPricingModelNegativeScenarioTest() throws Exception {
        exception.expect(IllegalCWSArgumentException.class);
        exception.expectMessage(expectedException);
        pricingModelFactory.getPricingModel(pricingConfig);
    }

}
