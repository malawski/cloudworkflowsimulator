package cws.core;

import java.util.Random;

/**
 * Returns 'runtime' +/- 'variance' percent of 'runtime', where the actual
 * variance is drawn from a uniform distribution. e.g. if 'variance' is .10,
 * then it will return a random uniform value that is +/- 10% of 'runtime'.
 * 
 * @author Gideon Juve <juve@usc.edu>
 */
public class UniformRuntimeDistribution implements RuntimeDistribution {
    private Random random;
    private double variance;

    public UniformRuntimeDistribution(long seed, double variance) {
        this.random = new Random(seed);
        this.variance = variance;
    }

    @Override
    public double getActualRuntime(double runtime) {
        // Get a random number in the range [-1,+1]
        double plusorminus = (random.nextDouble() * 2.0d) - 1.0d;
        return runtime + (plusorminus * variance * runtime);
    }

    public static void main(String[] args) {
        RuntimeDistribution d = new UniformRuntimeDistribution(0, 0.20);

        for (int i = 0; i < 1000; i++) {
            System.out.println(d.getActualRuntime(100));
        }
    }
}
