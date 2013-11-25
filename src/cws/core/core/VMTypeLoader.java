package cws.core.core;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.yaml.snakeyaml.Yaml;

public class VMTypeLoader {
    private static final String VMS_DIRECTORY = "vms/";

    private static final String VM_TYPE_OPTION_NAME = "vm";
    private static final String VM_TYPE_SHORT_OPTION_NAME = "vm";
    private static final String DEFAULT_VM_FILENAME = "default.vm.yaml";

    private static final String VM_CACHE_SIZE_OPTION_NAME = "vm-cache-size";
    private static final String VM_CACHE_SIZE_SHORT_OPTION_NAME = "vcs";

    // explanatory constant
    private static final boolean HAS_ARG = true;

    public VMType loadVM(Map<String, Object> config) throws MissingParameterException {
        if (!config.containsKey("mips") || !config.containsKey("cores") || !config.containsKey("cacheSize")) {
            throw new MissingParameterException();
        }

        Map<String, Object> billingConfig = (Map<String, Object>) config.get("billing");
        double unitPrice = ((Number) billingConfig.get("unitPrice")).doubleValue();
        double unitTime = ((Number) billingConfig.get("unitTime")).doubleValue();

        int mips = (int) config.get("mips");
        int cores = (int) config.get("cores");
        long cacheSize = ((Number) config.get("cacheSize")).longValue();

        return VMTypeBuilder.newBuilder().mips(mips).cores(cores).price(unitPrice).cacheSize(cacheSize)
                .billingTimeInSeconds(unitTime).build();
    }

    public static void buildCliOptions(Options options) {
        Option vm = new Option(VM_TYPE_SHORT_OPTION_NAME, VM_TYPE_OPTION_NAME, HAS_ARG, String.format(
                "VM config filename, relative to %s, defaults to %s", VMS_DIRECTORY, DEFAULT_VM_FILENAME));
        vm.setArgName("FILENAME");
        options.addOption(vm);

        Option delay = new Option("vpd", "vm-provisioning-delay", HAS_ARG, "VM provisioning delay time in seconds");
        delay.setArgName("DELAY IN SECONDS");
        options.addOption(delay);

        Option cacheSize = new Option(VM_CACHE_SIZE_SHORT_OPTION_NAME, VM_CACHE_SIZE_OPTION_NAME, HAS_ARG,
                "VM cache size");
        cacheSize.setArgName("SIZE");
        options.addOption(cacheSize);
    }

    public VMType determineVMType(CommandLine args) throws MissingParameterException, FileNotFoundException {
        // provisioningDelay = Double.parseDouble(args.getOptionValue("delay", DEFAULT_PROVISIONING_DELAY + ""));
        // cacheSize = Long.parseLong(args.getOptionValue("cache-size", DEFAULT_CACHE_SIZE + ""));
        // System.out.printf("delay = %f\n", provisioningDelay);
        // System.out.printf("cacheSize = %d\n", cacheSize);
        // if (provisioningDelay > 0.0) {
        // VMFactory.setProvisioningDelayDistribution(new ConstantDistribution(provisioningDelay));
        // }

        Map<String, Object> vmConfig = loadVMFromConfigFile(args);
        overrideConfigFromFileWithCliArgs(vmConfig, args);

        return loadVM(vmConfig);
    }

    private void overrideConfigFromFileWithCliArgs(Map<String, Object> vmConfig, CommandLine args) {
        // if(args.hasOption("vm-provisioning-delay")) {
        // vmConfig.put("vm-provisioning-delay", args.getOptionValue("vm-provisioning-delay"));
        // }

        if (args.hasOption(VM_CACHE_SIZE_OPTION_NAME)) {
            Long cacheSize = Long.parseLong(args.getOptionValue(VM_CACHE_SIZE_OPTION_NAME));
            vmConfig.put("cacheSize", cacheSize);
        }
    }

    private Map<String, Object> loadVMFromConfigFile(CommandLine args) throws FileNotFoundException {
        String vmConfigFilename = args.getOptionValue(VM_TYPE_OPTION_NAME, DEFAULT_VM_FILENAME);

        InputStream input = new FileInputStream(new File(VMS_DIRECTORY + vmConfigFilename));
        Yaml yaml = new Yaml();
        return (Map<String, Object>) yaml.load(input);
    }
}
