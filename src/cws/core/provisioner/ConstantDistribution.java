package cws.core.provisioner;

import org.cloudbus.cloudsim.distributions.ContinuousDistribution;

/**
 * @see ContinuousDistribution
 */
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
