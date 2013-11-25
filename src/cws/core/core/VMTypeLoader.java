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

public class VMTypeLoader {
    // explanatory constant
    private static final boolean HAS_ARG = true;

    private static final String VMS_DIRECTORY = "vms/";

    private static final String VM_TYPE_OPTION_NAME = "vm";
    private static final String VM_TYPE_SHORT_OPTION_NAME = "vm";
    private static final String DEFAULT_VM_FILENAME = "default.vm.yaml";

    private static final String VM_CACHE_SIZE_CONFIG_ENTRY = "cacheSize";
    private static final String VM_CACHE_SIZE_SHORT_OPTION_NAME = "vcs";
    private static final String VM_CACHE_SIZE_OPTION_NAME = "vm-cache-size";

    private static final String VM_MIPS_CONFIG_ENTRY = "mips";
    private static final String VM_MIPS_SHORT_OPTION_NAME = "vmi";
    private static final String VM_MIPS_OPTION_NAME = "vm-mips";

    private static final String VM_CORES_CONFIG_ENTRY = "cores";
    private static final String VM_CORES_SHORT_OPTION_NAME = "vco";
    private static final String VM_CORES_OPTION_NAME = "vm-cores";

    private static final String VM_BILLING_PRICE_CONFIG_ENTRY = "unitPrice";
    private static final String VM_BILLING_PRICE_SHORT_OPTION_NAME = "vbp";
    private static final String VM_BILLING_PRICE_OPTION_NAME = "vm-billing-price";

    private static final String VM_BILLING_TIME_CONFIG_ENTRY = "unitTime";
    private static final String VM_BILLING_UNIT_SHORT_OPTION_NAME = "vbu";
    private static final String VM_BILLING_UNIT_OPTION_NAME = "vm-billing-unit";

    private static final String VM_PROVISIONING_DELAY_DISTRIBUTION_SHORT_OPTION_NAME = "vpd";
    private static final String VM_PROVISIONING_DELAY_DISTRIBUTION_OPTION_NAME = "vm-provisioning-distribution";

    private static final String VM_DEPROVISIONING_DELAY_DISTRIBUTION_SHORT_OPTION_NAME = "vdd";
    private static final String VM_DEPROVISIONING_DELAY_DISTRIBUTION_OPTION_NAME = "vm-deprovisioning-distribution";

    private static final String VM_PROVISIONING_DELAY_VALUE_SHORT_OPTION_NAME = "vpv";
    private static final String VM_PROVISIONING_DELAY_VALUE_OPTION_NAME = "vm-provisioning-value";

    private static final String VM_DEPROVISIONING_DELAY_VALUE_SHORT_OPTION_NAME = "vdv";
    private static final String VM_DEPROVISIONING_DELAY_VALUE_OPTION_NAME = "vm-deprovisioning-value";

    private static final String DISTRIBUTION_TYPE_CONFIG_ENTRY = "distribution";
    private static final String DISTRIBUTION_VALUE_CONFIG_ENTRY = "value";

    public VMType loadVM(Map<String, Object> config) throws MissingParameterException, InvalidDistributionException {
        if (!config.containsKey(VM_MIPS_CONFIG_ENTRY) || !config.containsKey(VM_CORES_CONFIG_ENTRY)
                || !config.containsKey(VM_CACHE_SIZE_CONFIG_ENTRY)) {
            throw new MissingParameterException();
        }

        Map<String, Object> billingConfig = getBillingSection(config);
        double unitPrice = ((Number) billingConfig.get(VM_BILLING_PRICE_CONFIG_ENTRY)).doubleValue();
        double unitTime = ((Number) billingConfig.get(VM_BILLING_TIME_CONFIG_ENTRY)).doubleValue();

        int mips = (int) config.get(VM_MIPS_CONFIG_ENTRY);
        int cores = (int) config.get(VM_CORES_CONFIG_ENTRY);
        long cacheSize = ((Number) config.get(VM_CACHE_SIZE_CONFIG_ENTRY)).longValue();

        DistributionFactory factory = new DistributionFactory();

        Map<String, Object> provisioningConfig = getProvisioningSection(config);
        ContinuousDistribution provisioningDelay = factory.createDistribution(provisioningConfig);

        Map<String, Object> deprovisioningConfig = getDeprovisioningSection(config);
        ContinuousDistribution deprovisioningDelay = factory.createDistribution(deprovisioningConfig);

        return VMTypeBuilder.newBuilder().mips(mips).cores(cores).price(unitPrice).cacheSize(cacheSize)
                .billingTimeInSeconds(unitTime).provisioningTime(provisioningDelay)
                .deprovisioningTime(deprovisioningDelay).build();
    }

    public static void buildCliOptions(Options options) {
        Option vm = new Option(VM_TYPE_SHORT_OPTION_NAME, VM_TYPE_OPTION_NAME, HAS_ARG, String.format(
                "VM config filename, relative to %s, defaults to %s", VMS_DIRECTORY, DEFAULT_VM_FILENAME));
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

    public VMType determineVMType(CommandLine args) throws MissingParameterException, FileNotFoundException,
            InvalidDistributionException {
        Map<String, Object> vmConfig = loadVMFromConfigFile(args);
        overrideConfigFromFileWithCliArgs(vmConfig, args);

        return loadVM(vmConfig);
    }

    private void overrideConfigFromFileWithCliArgs(Map<String, Object> vmConfig, CommandLine args) {
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

        InputStream input = new FileInputStream(new File(VMS_DIRECTORY + vmConfigFilename));
        Yaml yaml = new Yaml();
        return (Map<String, Object>) yaml.load(input);
    }
}
