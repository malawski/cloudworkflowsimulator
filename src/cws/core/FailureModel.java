package cws.core;

import java.util.Random;

/**
 * This is a uniform failure distribution with a fixed failure rate.
 * 
 * @author Gideon Juve <juve@usc.edu>
 */
public class FailureModel {

    private Random random;

    private double failureRate;

    /**
     * @param seed Seed for random number generator
     * @param failureRate Failure rate between 0 and 1 representing percentage
     *            failures. A failure rate of 0 means that no failures occur.
     */
    public FailureModel(long seed, double failureRate) {
        this.random = new Random(seed);
        this.failureRate = failureRate;

        if (failureRate < 0 || failureRate > 1) {
            throw new IllegalArgumentException(String.format("Failure rate must be between 0 and 1, %f is not allowed",
                    failureRate));
        }
    }

    /**
     * This generates a true/false decision about whether a failure occurred
     * according to the failure rate.
     */
    public boolean failureOccurred() {
        double next = this.random.nextDouble();
        if (next < failureRate) {
            return true;
        }
        return false;
    }

    /**
     * This adjusts the runtime of a process that failed. Basically, it takes
     * the predicted runtime and chooses an arbitrary termination time within
     * the range [0, predictedRuntime)
     * 
     * @param predictedRuntime The predicted runtime of the process
     * @return A uniformly selected value in the range [0, predictedRuntime).
     */
    public double runtimeBeforeFailure(double predictedRuntime) {
        double fraction = this.random.nextDouble();
        return predictedRuntime * fraction;
    }
}
