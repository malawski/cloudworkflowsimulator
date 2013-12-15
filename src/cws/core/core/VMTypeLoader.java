package cws.core.core;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Map;

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
 * VM params can be overrode by CLI args like --vm-mips.
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
    static final String VM_CACHE_SIZE_SHORT_OPTION_NAME = "vcs";
    static final String VM_CACHE_SIZE_OPTION_NAME = "vm-cache-size";

    static final String VM_MIPS_CONFIG_ENTRY = "mips";
    static final String VM_MIPS_SHORT_OPTION_NAME = "vmi";
    static final String VM_MIPS_OPTION_NAME = "vm-mips";

    static final String VM_CORES_CONFIG_ENTRY = "cores";
    static final String VM_CORES_SHORT_OPTION_NAME = "vco";
    static final String VM_CORES_OPTION_NAME = "vm-cores";

    static final String VM_BILLING_PRICE_CONFIG_ENTRY = "unitPrice";
    static final String VM_BILLING_PRICE_SHORT_OPTION_NAME = "vbp";
    static final String VM_BILLING_PRICE_OPTION_NAME = "vm-billing-price";

    static final String VM_BILLING_TIME_CONFIG_ENTRY = "unitTime";
    static final String VM_BILLING_UNIT_SHORT_OPTION_NAME = "vbu";
    static final String VM_BILLING_UNIT_OPTION_NAME = "vm-billing-unit";

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
        } else if (!getBillingSection(config).containsKey(VM_BILLING_TIME_CONFIG_ENTRY)) {
            throw new IllegalCWSArgumentException("billing:unitTime configuration is missing in VM config file");
        } else if (!getBillingSection(config).containsKey(VM_BILLING_PRICE_CONFIG_ENTRY)) {
            throw new IllegalCWSArgumentException("billing:unitPrice configuration is missing in VM config file");
        } else if (!config.containsKey("provisioningDelay")) {
            throw new IllegalCWSArgumentException("provisioningDelay configuration is missing in VM config file");
        } else if (!config.containsKey("deprovisioningDelay")) {
            throw new IllegalCWSArgumentException("deprovisioningDelay configuration is missing in VM config file");
        }

        Map<String, Object> billingConfig = getBillingSection(config);
        double unitPrice = ((Number) billingConfig.get(VM_BILLING_PRICE_CONFIG_ENTRY)).doubleValue();
        double unitTime = ((Number) billingConfig.get(VM_BILLING_TIME_CONFIG_ENTRY)).doubleValue();

        int mips = ((Number) config.get(VM_MIPS_CONFIG_ENTRY)).intValue();
        int cores = ((Number) config.get(VM_CORES_CONFIG_ENTRY)).intValue();
        long cacheSize = ((Number) config.get(VM_CACHE_SIZE_CONFIG_ENTRY)).longValue();

        ContinuousDistributionFactory factory = new ContinuousDistributionFactory();

        Map<String, Object> provisioningConfig = getProvisioningSection(config);
        ContinuousDistribution provisioningDelay = loadDistribution(factory, provisioningConfig);

        Map<String, Object> deprovisioningConfig = getDeprovisioningSection(config);
        ContinuousDistribution deprovisioningDelay = loadDistribution(factory, deprovisioningConfig);

        return VMTypeBuilder.newBuilder().mips(mips).cores(cores).price(unitPrice).cacheSize(cacheSize)
                .billingTimeInSeconds(unitTime).provisioningTime(provisioningDelay)
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
                HAS_ARG, String.format(
                        "VM config directory, config files are loaded relatively to its path, defaults to %s",
                        DEFAULT_VM_CONFIGS_DIRECTORY));
        vmConfigDirectory.setArgName("DIRPATH");
        options.addOption(vmConfigDirectory);

        Option vm = new Option(VM_TYPE_SHORT_OPTION_NAME, VM_TYPE_OPTION_NAME, HAS_ARG, String.format(
                "VM config filename, defaults to %s", DEFAULT_VM_FILENAME));
        vm.setArgName("FILENAME");
        options.addOption(vm);

        Option cacheSize = new Option(VM_CACHE_SIZE_SHORT_OPTION_NAME, VM_CACHE_SIZE_OPTION_NAME, HAS_ARG,
                "Overrides VM cache size");
        cacheSize.setArgName("SIZE");
        options.addOption(cacheSize);

        Option mips = new Option(VM_MIPS_SHORT_OPTION_NAME, VM_MIPS_OPTION_NAME, HAS_ARG,
                "Overrides VM computational efficiency in mips units");
        mips.setArgName("NUMBER");
        options.addOption(mips);

        Option cores = new Option(VM_CORES_SHORT_OPTION_NAME, VM_CORES_OPTION_NAME, HAS_ARG,
                "Overrides VM cores number");
        cores.setArgName("NUMBER");
        options.addOption(cores);

        Option price = new Option(VM_BILLING_PRICE_SHORT_OPTION_NAME, VM_BILLING_PRICE_OPTION_NAME, HAS_ARG,
                "Overrides VM price per billing unit");
        price.setArgName("PRICE");
        options.addOption(price);

        Option billingUnit = new Option(VM_BILLING_UNIT_SHORT_OPTION_NAME, VM_BILLING_UNIT_OPTION_NAME, HAS_ARG,
                "Overrides VM billing unit in seconds");
        billingUnit.setArgName("SECONDS");
        options.addOption(billingUnit);

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

    public VMType determineVMType(CommandLine args) throws IllegalCWSArgumentException {
        Map<String, Object> vmConfig = tryLoadVMFromConfigFile(args);
        overrideConfigFromFileWithCliArgs(vmConfig, args);
        return loadVM(vmConfig);
    }

    private Map<String, Object> tryLoadVMFromConfigFile(CommandLine args) {
        try {
            return loadVMFromConfigFile(args);
        } catch (FileNotFoundException e) {
            throw new IllegalCWSArgumentException("Cannot load VM config file: " + e.getMessage());
        }
    }

    void overrideConfigFromFileWithCliArgs(Map<String, Object> vmConfig, CommandLine args) {
        if (args.hasOption(VM_MIPS_OPTION_NAME)) {
            Integer mips = Integer.parseInt(args.getOptionValue(VM_MIPS_OPTION_NAME));
            vmConfig.put(VM_MIPS_CONFIG_ENTRY, mips);
        }

        if (args.hasOption(VM_CORES_OPTION_NAME)) {
            Integer cores = Integer.parseInt(args.getOptionValue(VM_CORES_OPTION_NAME));
            vmConfig.put(VM_CORES_CONFIG_ENTRY, cores);
        }

        if (args.hasOption(VM_CACHE_SIZE_OPTION_NAME)) {
            Long cacheSize = Long.parseLong(args.getOptionValue(VM_CACHE_SIZE_OPTION_NAME));
            vmConfig.put(VM_CACHE_SIZE_CONFIG_ENTRY, cacheSize);
        }

        if (args.hasOption(VM_BILLING_PRICE_OPTION_NAME)) {
            Double billingPrice = Double.parseDouble(args.getOptionValue(VM_BILLING_PRICE_OPTION_NAME));
            Map<String, Object> billingConfig = getBillingSection(vmConfig);
            billingConfig.put(VM_BILLING_PRICE_CONFIG_ENTRY, billingPrice);
        }

        if (args.hasOption(VM_BILLING_UNIT_OPTION_NAME)) {
            Double billingUnit = Double.parseDouble(args.getOptionValue(VM_BILLING_UNIT_OPTION_NAME));
            Map<String, Object> billingConfig = getBillingSection(vmConfig);
            billingConfig.put(VM_BILLING_TIME_CONFIG_ENTRY, billingUnit);
        }

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
            Double distributionValue = Double.parseDouble(args
                    .getOptionValue(VM_DEPROVISIONING_DELAY_VALUE_OPTION_NAME));
            Map<String, Object> deprovisioningConfig = getDeprovisioningSection(vmConfig);
            deprovisioningConfig.put(DISTRIBUTION_VALUE_CONFIG_ENTRY, distributionValue);
        }

    }

    private Map<String, Object> getProvisioningSection(Map<String, Object> vmConfig) {
        return (Map<String, Object>) vmConfig.get("provisioningDelay");
    }

    private Map<String, Object> getDeprovisioningSection(Map<String, Object> config) {
        return (Map<String, Object>) config.get("deprovisioningDelay");
    }

    private Map<String, Object> getBillingSection(Map<String, Object> vmConfig) {
        return (Map<String, Object>) vmConfig.get("billing");
    }

    private Map<String, Object> loadVMFromConfigFile(CommandLine args) throws FileNotFoundException {
        String vmConfigFilename = args.getOptionValue(VM_TYPE_OPTION_NAME, DEFAULT_VM_FILENAME);
        String vmConfigDirectory = args.getOptionValue(VM_CONFIGS_DIRECTORY_OPTION_NAME, DEFAULT_VM_CONFIGS_DIRECTORY);

        InputStream input = new FileInputStream(new File(vmConfigDirectory, vmConfigFilename));
        Yaml yaml = new Yaml();
        return (Map<String, Object>) yaml.load(input);
    }
}
