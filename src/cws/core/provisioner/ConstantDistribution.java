package cws.core.provisioner;

import org.cloudbus.cloudsim.distributions.ContinuousDistribution;

import com.google.common.base.Objects;

/**
 * @see ContinuousDistribution
 */
public class ConstantDistribution implements ContinuousDistribution {
    private double delay;

    public ConstantDistribution(double delay) {
        this.delay = delay;
    }

    @Override
    public double sample() {
        return this.delay;
    }

    public String toString() {
        return "constant distribution, value = " + delay;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        final ConstantDistribution that = (ConstantDistribution) o;
        return Double.compare(that.delay, this.delay) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(this.delay);
    }
}
