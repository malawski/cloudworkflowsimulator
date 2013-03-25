package cws.core;

import java.util.Random;

import org.junit.Test;
import static org.junit.Assert.*;

public class TestFailureModel {

    private double computePercentFailures(double failureRate) {
        FailureModel m = new FailureModel(System.currentTimeMillis(), failureRate);

        int samples = 1000000;
        int failures = 0;
        for (int i = 0; i < samples; i++) {
            if (m.failureOccurred()) {
                failures++;
            }
        }

        return (double) failures / (double) samples;
    }

    @Test
    public void testFailureRate() {
        assertEquals(0.01, computePercentFailures(0.01), 0.001);
        assertEquals(0.05, computePercentFailures(0.05), 0.001);
        assertEquals(0.1, computePercentFailures(0.1), 0.001);
        assertEquals(0.5, computePercentFailures(0.5), 0.001);
        assertEquals(0.75, computePercentFailures(0.75), 0.001);
        assertEquals(1.0, computePercentFailures(1.0), 0.001);
    }

    @Test
    public void testActualRuntimes() {
        Random rand = new Random(System.currentTimeMillis());
        FailureModel m = new FailureModel(System.currentTimeMillis(), 0.0);
        for (int i = 0; i < 100000; i++) {
            double runtime = rand.nextDouble() * (i + 1);
            double actualRuntime = m.runtimeBeforeFailure(runtime);
            assertTrue(actualRuntime >= 0);
            assertTrue(String.format("%f < %f", actualRuntime, runtime), actualRuntime < runtime);
        }
    }
}
