package cws.core.pricing;

import org.apache.commons.cli.*;
import org.junit.Test;

import java.util.Map;

import static cws.core.pricing.PricingConfigLoader.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by Marcin Ziaber on 2016-10-24.
 */

public class PricingConfigLoaderPositiveTest {

    private final static PricingConfigLoader pricingConfigLoader = new PricingConfigLoader();

    @Test
    public void testLoadConfigFromFile() throws ParseException {
        String[] args = new String[] { "--pricing-file", "test.pricing.yaml", "--pricing-directory", "test/" };
        Options options = new Options();
        PricingConfigLoader.buildCliOptions(options);
        CommandLineParser parser = new PosixParser();
        CommandLine cmd = parser.parse(options, args);
        Map<String, Object> config = pricingConfigLoader.loadPricingModel(cmd);

        assertTrue(config.containsKey(MODEL_ENTRY));
        assertEquals("google", config.get(MODEL_ENTRY));

        assertTrue(config.containsKey(FIRST_BILLING_TIME_ENTRY));
        assertEquals(config.get(FIRST_BILLING_TIME_ENTRY), 600);

        assertTrue(config.containsKey(BILLING_TIME_ENTRY));
        assertEquals(config.get(BILLING_TIME_ENTRY), 120);

    }

    @Test
    public void testOverrideConfigFromFile() throws ParseException {
        String[] args = new String[] { "--pricing-file", "test.pricing.yaml", "--pricing-directory", "test/",
                "--first-billing-time-in-seconds", "1000", "--billing-time-in-seconds", "10" };
        Options options = new Options();
        PricingConfigLoader.buildCliOptions(options);
        CommandLineParser parser = new PosixParser();
        CommandLine cmd = parser.parse(options, args);
        Map<String, Object> config = pricingConfigLoader.loadPricingModel(cmd);

        assertTrue(config.containsKey(MODEL_ENTRY));
        assertEquals("google", config.get(MODEL_ENTRY));

        assertTrue(config.containsKey(FIRST_BILLING_TIME_ENTRY));
        assertEquals(config.get(FIRST_BILLING_TIME_ENTRY), 1000.);

        assertTrue(config.containsKey(BILLING_TIME_ENTRY));
        assertEquals(config.get(BILLING_TIME_ENTRY), 10.);

    }
}
