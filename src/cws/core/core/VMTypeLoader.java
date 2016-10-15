package cws.core.core;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.cloudbus.cloudsim.distributions.ContinuousDistribution;
import org.yaml.snakeyaml.Yaml;

import cws.core.exception.IllegalCWSArgumentException;

/**
 * Loads VMType from *.vm.yaml config files.
 * 
 * Uses --vm filename option. When the option is not specified
 * loads vms/default.vm.yaml file. VM files paths should be
 * specified relatively to vms/ directory by default.
 * 
 * VM provisioning and deprovisioning params for all VMs can be overrode by CLI args like --vm-provisioning-value.
 */
public class VMTypeLoader {
    // explanatory constant
    private static final boolean HAS_ARG = true;

    static final String VM_CONFIGS_DIRECTORY_OPTION_NAME = "vm-directory";
    static final String VM_CONFIGS_DIRECTORY_SHORT_OPTION_NAME = "vmd";
    static final String DEFAULT_VM_CONFIGS_DIRECTORY = "vms/";

    static final String VM_TYPE_OPTION_NAME = "vm";
    static final String VM_TYPE_SHORT_OPTION_NAME = "vm";
    static final String DEFAULT_VM_FILENAME = "default.vm.yaml";

    static final String VM_CACHE_SIZE_CONFIG_ENTRY = "cacheSize";

    static final String VM_MIPS_CONFIG_ENTRY = "mips";

    static final String VM_CORES_CONFIG_ENTRY = "cores";

    static final String VM_BILLING_PRICE_CONFIG_ENTRY = "unitPrice";

    static final String VM_PROVISIONING_DELAY_DISTRIBUTION_SHORT_OPTION_NAME = "vpd";
    static final String VM_PROVISIONING_DELAY_DISTRIBUTION_OPTION_NAME = "vm-provisioning-distribution";

    static final String VM_DEPROVISIONING_DELAY_DISTRIBUTION_SHORT_OPTION_NAME = "vdd";
    static final String VM_DEPROVISIONING_DELAY_DISTRIBUTION_OPTION_NAME = "vm-deprovisioning-distribution";

    static final String VM_PROVISIONING_DELAY_VALUE_SHORT_OPTION_NAME = "vpv";
    static final String VM_PROVISIONING_DELAY_VALUE_OPTION_NAME = "vm-provisioning-value";

    static final String VM_DEPROVISIONING_DELAY_VALUE_SHORT_OPTION_NAME = "vdv";
    static final String VM_DEPROVISIONING_DELAY_VALUE_OPTION_NAME = "vm-deprovisioning-value";

    static final String DISTRIBUTION_TYPE_CONFIG_ENTRY = "distribution";
    static final String DISTRIBUTION_VALUE_CONFIG_ENTRY = "value";

    VMType loadVM(Map<String, Object> config) throws IllegalCWSArgumentException {
        if (!config.containsKey(VM_MIPS_CONFIG_ENTRY)) {
            throw new IllegalCWSArgumentException("mips configuration is missing in VM config file");
        } else if (!config.containsKey(VM_CORES_CONFIG_ENTRY)) {
            throw new IllegalCWSArgumentException("cores configuration is missing in VM config file");
        } else if (!config.containsKey(VM_CACHE_SIZE_CONFIG_ENTRY)) {
            throw new IllegalCWSArgumentException("cache size configuration is missing in VM config file");
        } else if (!config.containsKey("billing")) {
            throw new IllegalCWSArgumentException("billing configuration is missing in VM config file");
        } else if (!getBillingSection(config).containsKey(VM_BILLING_PRICE_CONFIG_ENTRY)) {
            throw new IllegalCWSArgumentException("billing:unitPrice configuration is missing in VM config file");
        } else if (!config.containsKey("provisioningDelay")) {
            throw new IllegalCWSArgumentException("provisioningDelay configuration is missing in VM config file");
        } else if (!config.containsKey("deprovisioningDelay")) {
            throw new IllegalCWSArgumentException("deprovisioningDelay configuration is missing in VM config file");
        }

        Map<String, Object> billingConfig = getBillingSection(config);
        double unitPrice = ((Number) billingConfig.get(VM_BILLING_PRICE_CONFIG_ENTRY)).doubleValue();

        double mips = ((Number) config.get(VM_MIPS_CONFIG_ENTRY)).intValue();
        int cores = ((Number) config.get(VM_CORES_CONFIG_ENTRY)).intValue();
        long cacheSize = ((Number) config.get(VM_CACHE_SIZE_CONFIG_ENTRY)).longValue();

        ContinuousDistributionFactory factory = new ContinuousDistributionFactory();

        Map<String, Object> provisioningConfig = getProvisioningSection(config);
        ContinuousDistribution provisioningDelay = loadDistribution(factory, provisioningConfig);

        Map<String, Object> deprovisioningConfig = getDeprovisioningSection(config);
        ContinuousDistribution deprovisioningDelay = loadDistribution(factory, deprovisioningConfig);

        return VMTypeBuilder.newBuilder().mips(mips).cores(cores).price(unitPrice).cacheSize(cacheSize)
                .provisioningTime(provisioningDelay)
                .deprovisioningTime(deprovisioningDelay).build();
    }

    private ContinuousDistribution loadDistribution(ContinuousDistributionFactory factory,
            Map<String, Object> provisioningConfig) {
        try {
            return factory.createDistribution(provisioningConfig);
        } catch (InvalidDistributionException e) {
            throw new IllegalCWSArgumentException("Illegal argument for provisioning delay: " + e.getMessage());
        }
    }

    public static void buildCliOptions(Options options) {
        Option vmConfigDirectory = new Option(VM_CONFIGS_DIRECTORY_SHORT_OPTION_NAME, VM_CONFIGS_DIRECTORY_OPTION_NAME,
                HAS_ARG,
                String.format("VM config directory, config files are loaded relatively to its path, defaults to %s",
                        DEFAULT_VM_CONFIGS_DIRECTORY));
        vmConfigDirectory.setArgName("DIRPATH");
        options.addOption(vmConfigDirectory);

        Option vm = new Option(VM_TYPE_SHORT_OPTION_NAME, VM_TYPE_OPTION_NAME, HAS_ARG,
                String.format("VM config filename, defaults to %s", DEFAULT_VM_FILENAME));
        vm.setArgName("FILENAME");
        options.addOption(vm);

        Option provisioningDelayDistribution = new Option(VM_PROVISIONING_DELAY_DISTRIBUTION_SHORT_OPTION_NAME,
                VM_PROVISIONING_DELAY_DISTRIBUTION_OPTION_NAME, HAS_ARG,
                "Overrides VM provisioning distribution type (constant|uniform)");
        provisioningDelayDistribution.setArgName("TYPE");
        options.addOption(provisioningDelayDistribution);

        Option provisioningDelayValue = new Option(VM_PROVISIONING_DELAY_VALUE_SHORT_OPTION_NAME,
                VM_PROVISIONING_DELAY_VALUE_OPTION_NAME, HAS_ARG,
                "Overrides VM provisioning constant distribution value in seconds");
        provisioningDelayValue.setArgName("SECONDS");
        options.addOption(provisioningDelayValue);

        Option deprovisioningDelayDistribution = new Option(VM_DEPROVISIONING_DELAY_DISTRIBUTION_SHORT_OPTION_NAME,
                VM_DEPROVISIONING_DELAY_DISTRIBUTION_OPTION_NAME, HAS_ARG,
                "Overrides VM deprovisioning distribution type (constant|uniform)");
        deprovisioningDelayDistribution.setArgName("TYPE");
        options.addOption(deprovisioningDelayDistribution);

        Option deprovisioningDelayValue = new Option(VM_DEPROVISIONING_DELAY_VALUE_SHORT_OPTION_NAME,
                VM_DEPROVISIONING_DELAY_VALUE_OPTION_NAME, HAS_ARG,
                "Overrides VM deprovisioning constant distribution value in seconds");
        deprovisioningDelayValue.setArgName("SECONDS");
        options.addOption(deprovisioningDelayValue);
    }

    @SuppressWarnings("unchecked")
    public Set<VMType> determineVMTypes(CommandLine args) throws IllegalCWSArgumentException {
        final Iterable<Object> vmConfigs = tryLoadVMsFromConfigFile(args);
        final Set<VMType> vmTypes = new HashSet<VMType>();
        for (final Object vmConfig : vmConfigs) {
            final Map<String, Object> configMap = (Map<String, Object>) vmConfig;
            overrideConfigFromFileWithCliArgs(configMap, args);
            vmTypes.add(loadVM(configMap));
        }
        return vmTypes;
    }

    private Iterable<Object> tryLoadVMsFromConfigFile(CommandLine args) {
        try {
            return loadVMsFromConfigFile(args);
        } catch (FileNotFoundException e) {
            throw new IllegalCWSArgumentException("Cannot load VMs config file: " + e.getMessage());
        }
    }

    void overrideConfigFromFileWithCliArgs(Map<String, Object> vmConfig, CommandLine args) {
        if (args.hasOption(VM_PROVISIONING_DELAY_DISTRIBUTION_OPTION_NAME)) {
            String distributionType = args.getOptionValue(VM_PROVISIONING_DELAY_DISTRIBUTION_OPTION_NAME);
            Map<String, Object> provisioningConfig = getProvisioningSection(vmConfig);
            provisioningConfig.put(DISTRIBUTION_TYPE_CONFIG_ENTRY, distributionType);
        }

        if (args.hasOption(VM_PROVISIONING_DELAY_VALUE_OPTION_NAME)) {
            Double distributionValue = Double.parseDouble(args.getOptionValue(VM_PROVISIONING_DELAY_VALUE_OPTION_NAME));
            Map<String, Object> provisioningConfig = getProvisioningSection(vmConfig);
            provisioningConfig.put(DISTRIBUTION_VALUE_CONFIG_ENTRY, distributionValue);
        }

        if (args.hasOption(VM_DEPROVISIONING_DELAY_DISTRIBUTION_OPTION_NAME)) {
            String distributionType = args.getOptionValue(VM_DEPROVISIONING_DELAY_DISTRIBUTION_OPTION_NAME);
            Map<String, Object> deprovisioningConfig = getDeprovisioningSection(vmConfig);
            deprovisioningConfig.put(DISTRIBUTION_TYPE_CONFIG_ENTRY, distributionType);
        }

        if (args.hasOption(VM_DEPROVISIONING_DELAY_VALUE_OPTION_NAME)) {
            Double distributionValue = Double
                    .parseDouble(args.getOptionValue(VM_DEPROVISIONING_DELAY_VALUE_OPTION_NAME));
            Map<String, Object> deprovisioningConfig = getDeprovisioningSection(vmConfig);
            deprovisioningConfig.put(DISTRIBUTION_VALUE_CONFIG_ENTRY, distributionValue);
        }

    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> getProvisioningSection(Map<String, Object> vmConfig) {
        return (Map<String, Object>) vmConfig.get("provisioningDelay");
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> getDeprovisioningSection(Map<String, Object> config) {
        return (Map<String, Object>) config.get("deprovisioningDelay");
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> getBillingSection(Map<String, Object> vmConfig) {
        return (Map<String, Object>) vmConfig.get("billing");
    }

    private Iterable<Object> loadVMsFromConfigFile(CommandLine args) throws FileNotFoundException {
        String vmConfigFilename = args.getOptionValue(VM_TYPE_OPTION_NAME, DEFAULT_VM_FILENAME);
        String vmConfigDirectory = args.getOptionValue(VM_CONFIGS_DIRECTORY_OPTION_NAME, DEFAULT_VM_CONFIGS_DIRECTORY);

        InputStream input = new FileInputStream(new File(vmConfigDirectory, vmConfigFilename));
        Yaml yaml = new Yaml();
        return yaml.loadAll(input);
    }
}
