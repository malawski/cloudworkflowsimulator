package cws.core.core;

import java.util.HashMap;
import java.util.Map;

import junit.framework.Assert;

import org.cloudbus.cloudsim.distributions.UniformDistr;
import org.junit.Before;
import org.junit.Test;

import cws.core.exception.IllegalCWSArgumentException;
import cws.core.provisioner.ConstantDistribution;

public class VMTypeLoaderTest {

    private VMTypeLoader vmLoader;

    private Map<String, Object> config;
    private Map<String, Object> billingConfig;
    private Map<String, Object> provisioningConfig;
    private Map<String, Object> deprovisioningConfig;

    @Before
    public void setUp() {
        vmLoader = new VMTypeLoader();
        createValidConfig();
    }

    private void createValidConfig() {
        config = new HashMap<String, Object>();
        config.put("mips", 1);
        config.put("cores", 1);
        config.put("cacheSize", 1);

        billingConfig = new HashMap<String, Object>();
        billingConfig.put("unitTime", 1.0);
        billingConfig.put("unitPrice", 1.0);

        config.put("billing", billingConfig);

        provisioningConfig = new HashMap<String, Object>();
        provisioningConfig.put("value", 0.0);
        provisioningConfig.put("distribution", "constant");

        deprovisioningConfig = new HashMap<String, Object>();
        deprovisioningConfig.put("value", 0.0);
        deprovisioningConfig.put("distribution", "constant");

        config.put("provisioningDelay", provisioningConfig);
        config.put("deprovisioningDelay", deprovisioningConfig);
    }

    @Test
    public void shouldLoadMips() throws InvalidDistributionException {
        config.put("mips", 1234);

        VMType vmType = vmLoader.loadVM(config);

        Assert.assertEquals(1234, vmType.getMips());
    }

    @Test
    public void shouldLoadCores() {
        config.put("cores", 3);

        VMType vmType = vmLoader.loadVM(config);

        Assert.assertEquals(3, vmType.getCores());
    }

    @Test
    public void shouldLoadCacheSize() {
        config.put("cacheSize", 12345);

        VMType vmType = vmLoader.loadVM(config);

        Assert.assertEquals(12345, vmType.getCacheSize());

    }

    @Test
    public void shouldLoadPricing() {
        billingConfig.put("unitTime", 60.0);
        billingConfig.put("unitPrice", 2.4);

        VMType vmType = vmLoader.loadVM(config);

        Assert.assertEquals(60.0, vmType.getBillingTimeInSeconds());
        Assert.assertEquals(2.4, vmType.getPriceForBillingUnit());
    }

    @Test
    public void shouldLoadProvisioningDelay() {
        provisioningConfig.put("value", 22.0);
        provisioningConfig.put("distribution", "constant");

        VMType vmType = vmLoader.loadVM(config);

        Assert.assertEquals(22.0, vmType.getProvisioningDelay().sample());
        Assert.assertTrue(vmType.getProvisioningDelay() instanceof ConstantDistribution);
    }

    @Test
    public void shouldLoadDeprovisioningDelay() {
        deprovisioningConfig.put("minValue", 21.0);
        deprovisioningConfig.put("maxValue", 25.0);
        deprovisioningConfig.put("distribution", "uniform");

        VMType vmType = vmLoader.loadVM(config);

        Assert.assertTrue(vmType.getDeprovisioningDelay() instanceof UniformDistr);
    }

    @Test(expected = IllegalCWSArgumentException.class)
    public void shouldFailIfMipsIsMissing() throws InvalidDistributionException {
        config.remove("mips");

        vmLoader.loadVM(config);
    }

    @Test(expected = IllegalCWSArgumentException.class)
    public void shouldFailIfCoreIsMissing() throws InvalidDistributionException {
        config.remove("cores");

        vmLoader.loadVM(config);
    }

    @Test(expected = IllegalCWSArgumentException.class)
    public void shouldFailIfCacheIsMissing() throws InvalidDistributionException {
        config.remove("cacheSize");

        vmLoader.loadVM(config);
    }

    @Test(expected = IllegalCWSArgumentException.class)
    public void shouldFailIfBillingSectionIsMissing() {
        config.remove("billing");

        vmLoader.loadVM(config);
    }

    @Test(expected = IllegalCWSArgumentException.class)
    public void shouldFailIfPricingUnitIsMissing() {
        billingConfig.remove("unitTime");

        vmLoader.loadVM(config);
    }

    @Test(expected = IllegalCWSArgumentException.class)
    public void shouldFailIfUnitPriceIsMissing() {
        billingConfig.remove("unitPrice");

        vmLoader.loadVM(config);
    }

    @Test(expected = IllegalCWSArgumentException.class)
    public void shouldFailIfProvisioningDelaySectionIsMissing() {
        config.remove("provisioningDelay");

        vmLoader.loadVM(config);
    }

    @Test(expected = IllegalCWSArgumentException.class)
    public void shouldFailIfProvisioningDelayDistributionIsInvalid() {
        provisioningConfig.remove("distribution");

        vmLoader.loadVM(config);
    }

    @Test(expected = IllegalCWSArgumentException.class)
    public void shouldFailIfDeprovisioningDelaySectionIsMissing() {
        config.remove("deprovisioningDelay");

        vmLoader.loadVM(config);
    }

    @Test(expected = IllegalCWSArgumentException.class)
    public void shouldFailIfDeprovisioningDelayDistributionIsInvalid() {
        deprovisioningConfig.remove("distribution");

        vmLoader.loadVM(config);
    }

}
