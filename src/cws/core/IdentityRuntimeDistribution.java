package cws.core;

/**
 * Just returns the same runtime it was given
 * 
 * @author Gideon Juve <juve@usc.edu>
 */
public class IdentityRuntimeDistribution implements RuntimeDistribution {
    @Override
    public double getActualRuntime(double runtime) {
        return runtime;
    }
}
