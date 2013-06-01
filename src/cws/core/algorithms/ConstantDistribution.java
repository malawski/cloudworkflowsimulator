package cws.core.algorithms;

import org.cloudbus.cloudsim.distributions.ContinuousDistribution;

class ConstantDistribution implements ContinuousDistribution {
    private double delay;

    public ConstantDistribution(double delay) {
        this.delay = delay;
    }

    @Override
    public double sample() {
        return this.delay;
    }
}