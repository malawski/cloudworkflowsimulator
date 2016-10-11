package cws.core.pricing;

import cws.core.exception.IllegalCWSArgumentException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Map;

/**
 * Created by Marcin Ziaber on 2016-10-11.
 */
public class PricingConfigLoader {

    private static final String MODEL_ENTRY = "model";
    private static final String FIRST_BILLING_TIME_ENTRY = "firstBillingTimeInSeconds";
    private static final String BILLING_TIME_ENTRY = "billingTimeInSeconds";

    private static final String DEFAULT_PRICING_CONFIG_FILE_NAME = "default.pricing.yaml";
    private static final String DEFAULT_PRICING_CONFIG_FILE_DIRECTORY = "pricing/";

    private static final String PRICING_CONFIG_FILE_SHORT_OPTION_NAME = "pr";
    private static final String PRICING_CONFIG_FILE_OPTION_NAME = "pricing";
    private static final String PRICING_CONFIG_DIRECTORY_SHORT_OPTION_NAME = "prd";
    private static final String PRICING_CONFIG_DIRECTORY_OPTION_NAME = "pricing-directory";
    private static final String MODEL_SHORT_OPTION_NAME = "md";
    private static final String BILLING_TIME_SHORT_OPTION_NAME = "blt";
    private static final String BILLING_TIME_OPTION_NAME = "billing-time-in-seconds";
    private static final String FIRST_BILLING_TIME_SHORT_OPTION_NAME = "fblt";
    private static final String FIRST_BILLING_TIME_OPTION_NAME = "first-billing-time-in-seconds";

    private static final boolean HAS_ARG = true;


    public static void buildCliOptions(Options options) {
        Option pricingFilename = new Option(PRICING_CONFIG_FILE_SHORT_OPTION_NAME, PRICING_CONFIG_FILE_OPTION_NAME, HAS_ARG,
                String.format("Pricing config filename, defaults to %s", DEFAULT_PRICING_CONFIG_FILE_NAME));
        pricingFilename.setArgName("FILENAME");
        options.addOption(pricingFilename);

        Option pricingFilenameDirectory = new Option(PRICING_CONFIG_DIRECTORY_SHORT_OPTION_NAME,
                PRICING_CONFIG_DIRECTORY_OPTION_NAME, HAS_ARG,
                String.format("Pricing config directory, config files are loaded relatively to its path, defaults to %s",
                        DEFAULT_PRICING_CONFIG_FILE_DIRECTORY));
        pricingFilenameDirectory.setArgName("DIRPATH");
        options.addOption(pricingFilenameDirectory);

        Option model = new Option(MODEL_SHORT_OPTION_NAME, MODEL_ENTRY, HAS_ARG,
                "Overrides pricing model type");
        model.setArgName("MODEL");
        options.addOption(model);

        Option billingTimeInSeconds = new Option(BILLING_TIME_SHORT_OPTION_NAME, BILLING_TIME_OPTION_NAME, HAS_ARG,
                "Overrides pricing billingTimeInSeconds");
        billingTimeInSeconds.setArgName("BILLING_TIME_IN_SECONDS");
        options.addOption(billingTimeInSeconds);

        Option firstBillingTimeInSeconds = new Option(FIRST_BILLING_TIME_SHORT_OPTION_NAME, FIRST_BILLING_TIME_OPTION_NAME, HAS_ARG,
                "Overrides pricing billingTimeInSeconds");
        firstBillingTimeInSeconds.setArgName("FIRST_BILLING_TIME_IN_SECONDS");
        options.addOption(firstBillingTimeInSeconds);

    }

    public Map<String, Object> loadPricingModel(CommandLine args) {
        Map<String, Object> pricingConfig = tryLoadConfigFromFile(args);
        overrideConfigFromFileWithCliArgs(pricingConfig, args);
        return pricingConfig;
    }

    private Map<String, Object> tryLoadConfigFromFile(CommandLine args) {
        try {
            return loadConfigFromFile(args);
        } catch (FileNotFoundException e) {
            throw new IllegalCWSArgumentException("Cannot load pricing config file: " + e.getMessage());
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> loadConfigFromFile(CommandLine args) throws FileNotFoundException {
        String pricingConfigFilename = args.getOptionValue(PRICING_CONFIG_FILE_OPTION_NAME, DEFAULT_PRICING_CONFIG_FILE_NAME);
        String pricingConfigDirectory = args.getOptionValue(PRICING_CONFIG_DIRECTORY_OPTION_NAME, DEFAULT_PRICING_CONFIG_FILE_NAME);

        InputStream input = new FileInputStream(new File(pricingConfigDirectory, pricingConfigFilename));
        Yaml yaml = new Yaml();
        return (Map<String, Object>) yaml.load(input);
    }

    private void overrideConfigFromFileWithCliArgs(Map<String, Object> pricingConfig, CommandLine args) {
        overridePricingModel(pricingConfig, args);
        overrideBillingTimeInSeconds(pricingConfig, args);
        overrideFirstBillingTimeInSeconds(pricingConfig, args);
    }

    private void overridePricingModel(Map<String, Object> pricingConfig, CommandLine args) {
        if (args.hasOption(MODEL_ENTRY)) {
            pricingConfig.put(MODEL_ENTRY, args.getOptionValue(MODEL_ENTRY));
        }
    }

    private void overrideBillingTimeInSeconds(Map<String, Object> pricingConfig, CommandLine args) {
        if (args.hasOption(BILLING_TIME_OPTION_NAME)) {
            try {
                Double billingTimeInSeconds = Double.parseDouble(args.getOptionValue(BILLING_TIME_OPTION_NAME));
                pricingConfig.put(BILLING_TIME_ENTRY, billingTimeInSeconds);
            } catch (NumberFormatException e) {
                throw new IllegalCWSArgumentException(BILLING_TIME_ENTRY
                        + " was overrode with a non-number value");
            }
        }
    }

    private void overrideFirstBillingTimeInSeconds(Map<String, Object> pricingConfig, CommandLine args) {
        if (args.hasOption(FIRST_BILLING_TIME_OPTION_NAME)) {
            try {
                Double firstBillingTimeInSeconds = Double.parseDouble(args.getOptionValue(FIRST_BILLING_TIME_OPTION_NAME));
                pricingConfig.put(FIRST_BILLING_TIME_ENTRY, firstBillingTimeInSeconds);
            } catch (NumberFormatException e) {
                throw new IllegalCWSArgumentException(FIRST_BILLING_TIME_ENTRY
                        + " was overrode with a non-number value");
            }
        }
    }
}