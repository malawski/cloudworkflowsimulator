package cws.core.core;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;

import org.apache.commons.cli.*;
import org.junit.Before;
import org.junit.Test;

public class VMTypeLoaderCliTest {
    private HashMap<String, Object> config;
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
