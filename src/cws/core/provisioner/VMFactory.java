package cws.core.provisioner;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import cws.core.FailureModel;
import cws.core.VM;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.core.VMType;
import cws.core.jobs.IdentityRuntimeDistribution;
import cws.core.jobs.RuntimeDistribution;
import cws.core.jobs.UniformRuntimeDistribution;

public class VMFactory {
    private static final double DEFAULT_RUNTIME_VARIANCE = 0.0;
    private static final double DEFAULT_FAILURE_RATE = 0.0;

    private static RuntimeDistribution runtimeDistribution = new IdentityRuntimeDistribution();
    private static FailureModel failureModel = new FailureModel(0, 0.0);
    private static double runtimeVariance;
    private static double failureRate;

    public static void setRuntimeDistribution(RuntimeDistribution runtimeDistribution) {
        VMFactory.runtimeDistribution = runtimeDistribution;
    }

    public static RuntimeDistribution getRuntimeDistribution() {
        return runtimeDistribution;
    }

    public static FailureModel getFailureModel() {
        return failureModel;
    }

    public static void setFailureModel(FailureModel failureModel) {
        VMFactory.failureModel = failureModel;
    }

    /**
     * @param cloudSimWrapper - initialized CloudSimWrapper instance. It needs to be inited, because we're creting
     *            storage manager here.
     */
    public static VM createVM(VMType vmType, CloudSimWrapper cloudSimWrapper) {
        VM vm = new VM(vmType, cloudSimWrapper);
        vm.setRuntimeDistribution(runtimeDistribution);
        vm.setFailureModel(failureModel);
        return vm;
    }

    public static void buildCliOptions(Options options) {
        Option runtimeVariance = new Option("rv", "runtime-variance", true, "Runtime variance, defaults to "
                + DEFAULT_RUNTIME_VARIANCE);
        runtimeVariance.setArgName("VAR");
        options.addOption(runtimeVariance);

        // TODO(mequrel): should be moved to TestRun somewhere
        // Option delay = new Option("dl", "delay", true, "Delay, defaluts to " + DEFAULT_PROVISIONING_DELAY);
        // delay.setArgName("DELAY");
        // options.addOption(delay);

        // TODO(mequrel): similar as above
        // Option cacheSize = new Option("cs", "cache-size", true, "VM cache size, defaluts to " + DEFAULT_CACHE_SIZE
        // + " bytes");
        // cacheSize.setArgName("SIZE");
        // options.addOption(cacheSize);

        Option failureRate = new Option("fr", "failure-rate", true, "Faliure rate, defaults to " + DEFAULT_FAILURE_RATE);
        failureRate.setArgName("RATE");
        options.addOption(failureRate);
    }

    public static void readCliOptions(CommandLine args, long seed) {
        runtimeVariance = Double.parseDouble(args.getOptionValue("runtime-variance", DEFAULT_RUNTIME_VARIANCE + ""));
        // provisioningDelay = Double.parseDouble(args.getOptionValue("delay", DEFAULT_PROVISIONING_DELAY + ""));
        failureRate = Double.parseDouble(args.getOptionValue("failure-rate", DEFAULT_FAILURE_RATE + ""));
        // cacheSize = Long.parseLong(args.getOptionValue("cache-size", DEFAULT_CACHE_SIZE + ""));

        System.out.printf("runtimeVariance = %f\n", runtimeVariance);
        // System.out.printf("delay = %f\n", provisioningDelay);
        System.out.printf("failureRate = %f\n", failureRate);
        // System.out.printf("cacheSize = %d\n", cacheSize);

        if (runtimeVariance > 0.0) {
            VMFactory.setRuntimeDistribution(new UniformRuntimeDistribution(seed, runtimeVariance));
        }

        // if (provisioningDelay > 0.0) {
        // VMFactory.setProvisioningDelayDistribution(new ConstantDistribution(provisioningDelay));
        // }

        if (failureRate > 0.0) {
            VMFactory.setFailureModel(new FailureModel(seed, failureRate));
        }
    }

    public static double getRuntimeVariance() {
        return runtimeVariance;
    }

    public static double getFailureRate() {
        return failureRate;
    }
}
