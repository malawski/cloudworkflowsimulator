package cws.core.core;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.junit.Before;
import org.junit.Test;

public class VMTypeLoaderCliTest {
    private HashMap<String, Object> config;
    private HashMap<String, Object> billingConfig;
    private HashMap<String, Object> provisioningConfig;
    private HashMap<String, Object> deprovisioningConfig;
    private VMTypeLoader vmTypeLoader;

    @Before
    public void setUp() {
        vmTypeLoader = new VMTypeLoader();

        createValidConfig();
    }

    private void createValidConfig() {
        config = new HashMap<String, Object>();
        config.put("mips", 1.0);
        config.put("cores", 1);
        config.put("cacheSize", 1);

        billingConfig = new HashMap<String, Object>();
        billingConfig.put("unitTime", 1.0);
        billingConfig.put("unitPrice", 1.0);

        config.put("billing", billingConfig);

        provisioningConfig = new HashMap<String, Object>();
        provisioningConfig.put("value", 0.0);
        provisioningConfig.put("distribution", "constant");

        deprovisioningConfig = new HashMap<String, Object>();
        deprovisioningConfig.put("value", 0.0);
        deprovisioningConfig.put("distribution", "constant");

        config.put("provisioningDelay", provisioningConfig);
        config.put("deprovisioningDelay", deprovisioningConfig);
    }

    private String makeOption(String optionString) {
        return "--" + optionString;
    }

    private String makeShortOption(String optionString) {
        return "-" + optionString;
    }

    private CommandLine parseArgs(String[] args) throws ParseException {
        Options options = new Options();
        VMTypeLoader.buildCliOptions(options);
        CommandLineParser parser = new PosixParser();
        return parser.parse(options, args);
    }

    @Test
    public void shouldEnableToOverrideCacheSize() throws ParseException {
        CommandLine cmd = parseArgs(new String[] { makeOption(VMTypeLoader.VM_CACHE_SIZE_OPTION_NAME), "12345" });

        vmTypeLoader.overrideConfigFromFileWithCliArgs(config, cmd);

        assertEquals(12345L, config.get(VMTypeLoader.VM_CACHE_SIZE_CONFIG_ENTRY));
    }

    @Test
    public void shouldEnableToOverrideCacheSizeWithShortOption() throws ParseException {
        CommandLine cmd = parseArgs(new String[] { makeShortOption(VMTypeLoader.VM_CACHE_SIZE_SHORT_OPTION_NAME),
                "12345" });

        vmTypeLoader.overrideConfigFromFileWithCliArgs(config, cmd);

        assertEquals(12345L, config.get(VMTypeLoader.VM_CACHE_SIZE_CONFIG_ENTRY));
    }

    @Test
    public void shouldEnableToOverrideMips() throws ParseException {
        CommandLine cmd = parseArgs(new String[] { makeOption(VMTypeLoader.VM_MIPS_OPTION_NAME), "1000.0" });

        vmTypeLoader.overrideConfigFromFileWithCliArgs(config, cmd);

        assertEquals(1000.0, config.get(VMTypeLoader.VM_MIPS_CONFIG_ENTRY));
    }

    @Test
    public void shouldEnableToOverrideMipsWithShortOption() throws ParseException {
        CommandLine cmd = parseArgs(new String[] { makeShortOption(VMTypeLoader.VM_MIPS_SHORT_OPTION_NAME), "1000.0" });

        vmTypeLoader.overrideConfigFromFileWithCliArgs(config, cmd);

        assertEquals(1000.0, config.get(VMTypeLoader.VM_MIPS_CONFIG_ENTRY));
    }

    @Test
    public void shouldEnableToOverrideCores() throws ParseException {
        CommandLine cmd = parseArgs(new String[] { makeOption(VMTypeLoader.VM_CORES_OPTION_NAME), "3" });

        vmTypeLoader.overrideConfigFromFileWithCliArgs(config, cmd);

        assertEquals(3, config.get(VMTypeLoader.VM_CORES_CONFIG_ENTRY));
    }

    @Test
    public void shouldEnableToOverrideCoresWithShortOption() throws ParseException {
        CommandLine cmd = parseArgs(new String[] { makeShortOption(VMTypeLoader.VM_CORES_SHORT_OPTION_NAME), "3" });

        vmTypeLoader.overrideConfigFromFileWithCliArgs(config, cmd);

        assertEquals(3, config.get(VMTypeLoader.VM_CORES_CONFIG_ENTRY));
    }

    @Test
    public void shouldEnableToOverridePricingUnit() throws ParseException {
        CommandLine cmd = parseArgs(new String[] { makeOption(VMTypeLoader.VM_BILLING_UNIT_OPTION_NAME), "30.0" });

        vmTypeLoader.overrideConfigFromFileWithCliArgs(config, cmd);

        assertEquals(30.0, billingConfig.get(VMTypeLoader.VM_BILLING_TIME_CONFIG_ENTRY));
    }

    @Test
    public void shouldEnableToOverridePricingUnitWithShortOption() throws ParseException {
        CommandLine cmd = parseArgs(new String[] { makeShortOption(VMTypeLoader.VM_BILLING_UNIT_SHORT_OPTION_NAME),
                "30.0" });

        vmTypeLoader.overrideConfigFromFileWithCliArgs(config, cmd);

        assertEquals(30.0, billingConfig.get(VMTypeLoader.VM_BILLING_TIME_CONFIG_ENTRY));
    }

    @Test
    public void shouldEnableToOverridePricePerUnit() throws ParseException {
        CommandLine cmd = parseArgs(new String[] { makeOption(VMTypeLoader.VM_BILLING_PRICE_OPTION_NAME), "2.3" });

        vmTypeLoader.overrideConfigFromFileWithCliArgs(config, cmd);

        assertEquals(2.3, billingConfig.get(VMTypeLoader.VM_BILLING_PRICE_CONFIG_ENTRY));
    }

    @Test
    public void shouldEnableToOverridePricePerUnitWithShortOption() throws ParseException {
        CommandLine cmd = parseArgs(new String[] { makeShortOption(VMTypeLoader.VM_BILLING_PRICE_SHORT_OPTION_NAME),
                "2.3" });

        vmTypeLoader.overrideConfigFromFileWithCliArgs(config, cmd);

        assertEquals(2.3, billingConfig.get(VMTypeLoader.VM_BILLING_PRICE_CONFIG_ENTRY));
    }

    @Test
    public void shouldEnableToOverrideProvisioningDelayDistribution() throws ParseException {
        CommandLine cmd = parseArgs(new String[] {
                makeOption(VMTypeLoader.VM_PROVISIONING_DELAY_DISTRIBUTION_OPTION_NAME), "a_distribution" });

        vmTypeLoader.overrideConfigFromFileWithCliArgs(config, cmd);

        assertEquals("a_distribution", provisioningConfig.get(VMTypeLoader.DISTRIBUTION_TYPE_CONFIG_ENTRY));
    }

    @Test
    public void shouldEnableToOverrideProvisioningDelayDistributionWithShortOption() throws ParseException {
        CommandLine cmd = parseArgs(new String[] {
                makeShortOption(VMTypeLoader.VM_PROVISIONING_DELAY_DISTRIBUTION_SHORT_OPTION_NAME), "a_distribution" });

        vmTypeLoader.overrideConfigFromFileWithCliArgs(config, cmd);

        assertEquals("a_distribution", provisioningConfig.get(VMTypeLoader.DISTRIBUTION_TYPE_CONFIG_ENTRY));
    }

    @Test
    public void shouldEnableToOverrideProvisioningDelayValue() throws ParseException {
        CommandLine cmd = parseArgs(new String[] { makeOption(VMTypeLoader.VM_PROVISIONING_DELAY_VALUE_OPTION_NAME),
                "12.3" });

        vmTypeLoader.overrideConfigFromFileWithCliArgs(config, cmd);

        assertEquals(12.3, provisioningConfig.get(VMTypeLoader.DISTRIBUTION_VALUE_CONFIG_ENTRY));
    }

    @Test
    public void shouldEnableToOverrideProvisioningDelayValueWithShortOption() throws ParseException {
        CommandLine cmd = parseArgs(new String[] {
                makeShortOption(VMTypeLoader.VM_PROVISIONING_DELAY_VALUE_SHORT_OPTION_NAME), "12.3" });

        vmTypeLoader.overrideConfigFromFileWithCliArgs(config, cmd);

        assertEquals(12.3, provisioningConfig.get(VMTypeLoader.DISTRIBUTION_VALUE_CONFIG_ENTRY));
    }

    @Test
    public void shouldEnableToOverrideDeprovisioningDelayDistribution() throws ParseException {
        CommandLine cmd = parseArgs(new String[] {
                makeOption(VMTypeLoader.VM_DEPROVISIONING_DELAY_DISTRIBUTION_OPTION_NAME), "a_distribution" });

        vmTypeLoader.overrideConfigFromFileWithCliArgs(config, cmd);

        assertEquals("a_distribution", deprovisioningConfig.get(VMTypeLoader.DISTRIBUTION_TYPE_CONFIG_ENTRY));
    }

    @Test
    public void shouldEnableToOverrideDeprovisioningDelayDistributionWithShortOption() throws ParseException {
        CommandLine cmd = parseArgs(new String[] {
                makeShortOption(VMTypeLoader.VM_DEPROVISIONING_DELAY_DISTRIBUTION_SHORT_OPTION_NAME), "a_distribution" });

        vmTypeLoader.overrideConfigFromFileWithCliArgs(config, cmd);

        assertEquals("a_distribution", deprovisioningConfig.get(VMTypeLoader.DISTRIBUTION_TYPE_CONFIG_ENTRY));
    }

    @Test
    public void shouldEnableToOverrideDeprovisioningDelayValue() throws ParseException {
        CommandLine cmd = parseArgs(new String[] { makeOption(VMTypeLoader.VM_DEPROVISIONING_DELAY_VALUE_OPTION_NAME),
                "12.3" });

        vmTypeLoader.overrideConfigFromFileWithCliArgs(config, cmd);

        assertEquals(12.3, deprovisioningConfig.get(VMTypeLoader.DISTRIBUTION_VALUE_CONFIG_ENTRY));
    }

    @Test
    public void shouldEnableToOverrideDeprovisioningDelayValueWithShortOption() throws ParseException {
        CommandLine cmd = parseArgs(new String[] {
                makeShortOption(VMTypeLoader.VM_DEPROVISIONING_DELAY_VALUE_SHORT_OPTION_NAME), "12.3" });

        vmTypeLoader.overrideConfigFromFileWithCliArgs(config, cmd);

        assertEquals(12.3, deprovisioningConfig.get(VMTypeLoader.DISTRIBUTION_VALUE_CONFIG_ENTRY));
    }

}
