package cws.core.core;

import java.util.Map;

import org.cloudbus.cloudsim.distributions.ContinuousDistribution;
import org.cloudbus.cloudsim.distributions.UniformDistr;

import cws.core.provisioner.ConstantDistribution;

public class DistributionFactory {
    public ContinuousDistribution createDistribution(Map<String, Object> distributionConfig)
            throws InvalidDistributionException {
        String distributionType = getDistributionType(distributionConfig);

        if ("constant".equals(distributionType)) {
            return tryCreateConstantDistribution(distributionConfig);
        } else if ("uniform".equals(distributionType)) {
            return tryCreateUniformDistribution(distributionConfig);
        }

        throw new InvalidDistributionException("Bad distribution type given");
    }

    private String getDistributionType(Map<String, Object> distributionConfig) throws InvalidDistributionException {
        if (!distributionConfig.containsKey("distribution")) {
            throw new InvalidDistributionException("Distribution type missing");
        }

        return (String) distributionConfig.get("distribution");
    }

    private ContinuousDistribution tryCreateConstantDistribution(Map<String, Object> distributionConfig)
            throws InvalidDistributionException {
        double value = getConstantDistributionValue(distributionConfig);
        return new ConstantDistribution(value);
    }

    private double getConstantDistributionValue(Map<String, Object> distributionConfig)
            throws InvalidDistributionException {
        if (!distributionConfig.containsKey("value")) {
            throw new InvalidDistributionException("Value param for constant distribution is missing");
        }
        if (!(distributionConfig.get("value") instanceof Number)) {
            throw new InvalidDistributionException("Value param for constant distribution is invalid");
        }

        return ((Number) distributionConfig.get("value")).doubleValue();
    }

    private ContinuousDistribution tryCreateUniformDistribution(Map<String, Object> distributionConfig)
            throws InvalidDistributionException {
        if (!distributionConfig.containsKey("minValue")) {
            throw new InvalidDistributionException("minValue param for uniform distribution is missing");
        }
        if (!distributionConfig.containsKey("maxValue")) {
            throw new InvalidDistributionException("maxValue param for uniform distribution is missing");
        }
        if (!(distributionConfig.get("minValue") instanceof Number)) {
            throw new InvalidDistributionException("minValue param for uniform distribution is invalid");
        }
        if (!(distributionConfig.get("maxValue") instanceof Number)) {
            throw new InvalidDistributionException("maxValue param for constant distribution is invalid");
        }

        try {
            return createUniformDistribution(distributionConfig);
        } catch (IllegalArgumentException e) {
            throw new InvalidDistributionException("Bad params for constant distribution: " + e.getMessage());
        }
    }

    private ContinuousDistribution createUniformDistribution(Map<String, Object> distributionConfig) {
        double minValue = ((Number) distributionConfig.get("minValue")).doubleValue();
        double maxValue = ((Number) distributionConfig.get("maxValue")).doubleValue();
        return new UniformDistr(minValue, maxValue);
    }
}
