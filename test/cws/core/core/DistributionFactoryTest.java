package cws.core.core;

import java.util.HashMap;
import java.util.Map;

import junit.framework.Assert;

import org.cloudbus.cloudsim.distributions.ContinuousDistribution;
import org.cloudbus.cloudsim.distributions.UniformDistr;
import org.junit.Before;
import org.junit.Test;

public class DistributionFactoryTest {
    private DistributionFactory factory;
    private Map<String, Object> distributionConfig;

    @Before
    public void setUp() {
        factory = new DistributionFactory();
        distributionConfig = new HashMap<String, Object>();
    }

    @Test(expected = InvalidDistributionException.class)
    public void shouldFailIfDistributionTypeNotGiven() throws InvalidDistributionException {
        factory.createDistribution(distributionConfig);
    }

    @Test
    public void shouldCreateConstantDistributionWhenDoubleValueGiven() throws InvalidDistributionException {
        distributionConfig.put("distribution", "constant");
        distributionConfig.put("value", 10.2);

        ContinuousDistribution distribution = factory.createDistribution(distributionConfig);

        Assert.assertEquals(10.2, distribution.sample());
    }

    @Test(expected = InvalidDistributionException.class)
    public void shouldNotCreateConstantDistributionWhenValueMissing() throws InvalidDistributionException {
        distributionConfig.put("distribution", "constant");

        factory.createDistribution(distributionConfig);
    }

    @Test(expected = InvalidDistributionException.class)
    public void shouldNotCreateConstantDistributionWhenValueInvalid() throws InvalidDistributionException {
        distributionConfig.put("distribution", "constant");
        distributionConfig.put("value", "invalid");

        factory.createDistribution(distributionConfig);
    }

    @Test
    public void shouldCreateUniformDistribution() throws InvalidDistributionException {
        distributionConfig.put("distribution", "uniform");
        distributionConfig.put("minValue", 10);
        distributionConfig.put("maxValue", 12.2);

        ContinuousDistribution distribution = factory.createDistribution(distributionConfig);

        Assert.assertTrue("bad type of distribution", distribution instanceof UniformDistr);
        double exampleValue = distribution.sample();
        Assert.assertTrue("example value was out of range", 10.0 <= exampleValue && exampleValue <= 12.2);
    }

    @Test(expected = InvalidDistributionException.class)
    public void shouldNotCreateUniformDistributionWithMissingMaxValue() throws InvalidDistributionException {
        distributionConfig.put("distribution", "uniform");
        distributionConfig.put("maxValue", 12.2);

        factory.createDistribution(distributionConfig);
    }

    @Test(expected = InvalidDistributionException.class)
    public void shouldNotCreateUniformDistributionWithMissingMinValue() throws InvalidDistributionException {
        distributionConfig.put("distribution", "uniform");
        distributionConfig.put("minValue", 10);

        factory.createDistribution(distributionConfig);
    }

    @Test(expected = InvalidDistributionException.class)
    public void shouldNotCreateUniformDistributionWithInvalidMinValue() throws InvalidDistributionException {
        distributionConfig.put("distribution", "uniform");
        distributionConfig.put("minValue", "invalid");
        distributionConfig.put("maxValue", 12.2);

        factory.createDistribution(distributionConfig);
    }

    @Test(expected = InvalidDistributionException.class)
    public void shouldNotCreateUniformDistributionWithInvalidMaxValue() throws InvalidDistributionException {
        distributionConfig.put("distribution", "uniform");
        distributionConfig.put("minValue", 10);
        distributionConfig.put("maxValue", "invalid");

        factory.createDistribution(distributionConfig);
    }

    @Test(expected = InvalidDistributionException.class)
    public void shouldNotCreateUniformDistributionWithInvalidOptions() throws InvalidDistributionException {
        distributionConfig.put("distribution", "uniform");
        distributionConfig.put("minValue", 15);
        distributionConfig.put("maxValue", 12);

        factory.createDistribution(distributionConfig);
    }

    @Test(expected = InvalidDistributionException.class)
    public void shouldFailIfDistributionTypeIsInvalid() throws InvalidDistributionException {
        distributionConfig.put("distribution", "invalid");

        factory.createDistribution(distributionConfig);
    }

}
