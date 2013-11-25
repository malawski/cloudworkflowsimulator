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

        Option failureRate = new Option("fr", "failure-rate", true, "Faliure rate, defaults to " + DEFAULT_FAILURE_RATE);
        failureRate.setArgName("RATE");
        options.addOption(failureRate);
    }

    public static void readCliOptions(CommandLine args, long seed) {
        runtimeVariance = Double.parseDouble(args.getOptionValue("runtime-variance", DEFAULT_RUNTIME_VARIANCE + ""));
        failureRate = Double.parseDouble(args.getOptionValue("failure-rate", DEFAULT_FAILURE_RATE + ""));

        System.out.printf("runtimeVariance = %f\n", runtimeVariance);
        System.out.printf("failureRate = %f\n", failureRate);

        if (runtimeVariance > 0.0) {
            VMFactory.setRuntimeDistribution(new UniformRuntimeDistribution(seed, runtimeVariance));
        }

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
