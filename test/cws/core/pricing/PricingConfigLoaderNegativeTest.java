package cws.core.pricing;

import cws.core.exception.IllegalCWSArgumentException;
import org.apache.commons.cli.*;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static cws.core.pricing.PricingConfigLoader.BILLING_TIME_ENTRY;
import static cws.core.pricing.PricingConfigLoader.FIRST_BILLING_TIME_ENTRY;

/**
 * Created by Marcin Ziaber on 2016-10-24.
 */
@RunWith(Parameterized.class)

public class PricingConfigLoaderNegativeTest {
    private String[] args;
    private String expectedExceptionMessage;

    private final static PricingConfigLoader pricingConfigLoader = new PricingConfigLoader();

    public PricingConfigLoaderNegativeTest(String[] args, String expectedExceptionMessage) {
        this.args = args;
        this.expectedExceptionMessage = expectedExceptionMessage;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {new String[]{"--pricing-file", "badfile", "--pricing-directory", "test/"}, null},
                {new String[]{"--pricing-file", "test.pricing.yaml", "--pricing-directory", "test/", "--billing-time-in-seconds", "test"}, BILLING_TIME_ENTRY
                        + " was overrode with a non-number value"},
                {new String[]{"--pricing-file", "test.pricing.yaml", "--pricing-directory", "test/", "--first-billing-time-in-seconds", "test"}, FIRST_BILLING_TIME_ENTRY
                        + " was overrode with a non-number value"}

        });
    }

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void testNegativePricingModel() throws ParseException {
        Options options = new Options();
        PricingConfigLoader.buildCliOptions(options);
        CommandLineParser parser = new PosixParser();
        CommandLine cmd = parser.parse(options, args);
        exception.expect(IllegalCWSArgumentException.class);
        if (expectedExceptionMessage != null) {
            exception.expectMessage(expectedExceptionMessage);
        }
        pricingConfigLoader.loadPricingModel(cmd);
    }

}