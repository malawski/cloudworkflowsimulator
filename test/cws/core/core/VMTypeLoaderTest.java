package cws.core.core;

import java.util.HashMap;
import java.util.Map;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

public class VMTypeLoaderTest {

    private VMTypeLoader vmLoader;
    private Map<String, Object> validConfig;

    @Before
    public void setUp() {
        vmLoader = new VMTypeLoader();

        validConfig = createValidConfig();
    }

    private Map<String, Object> createValidConfig() {
        Map<String, Object> config = new HashMap<String, Object>();
        config.put("mips", 1);
        config.put("cores", 1);
        config.put("cacheSize", 1);

        Map<String, Object> billingConfig = new HashMap<String, Object>();
        billingConfig.put("unitTime", 1.0);
        billingConfig.put("unitPrice", 1.0);

        config.put("billing", billingConfig);

        return config;
    }

    @Test
    public void shouldLoadBasicParams() throws MissingParameterException {
        Map<String, Object> config = new HashMap<String, Object>();
        config.put("mips", 1000);
        config.put("cores", 1);
        config.put("cacheSize", 12345);

        Map<String, Object> billingConfig = new HashMap<String, Object>();
        billingConfig.put("unitTime", 60.0);
        billingConfig.put("unitPrice", 2.4);

        config.put("billing", billingConfig);

        VMType vmType = vmLoader.loadVM(config);

        Assert.assertEquals(1000, vmType.getMips());
        Assert.assertEquals(1, vmType.getCores());
        Assert.assertEquals(12345, vmType.getCacheSize());

        Assert.assertEquals(60.0, vmType.getBillingTimeInSeconds());
        Assert.assertEquals(2.4, vmType.getPriceForBillingUnit());
    }

    @Test(expected = MissingParameterException.class)
    public void shouldFailIfMipsIsMissing() throws MissingParameterException {
        Map<String, Object> config = createValidConfig();
        config.remove("mips");

        vmLoader.loadVM(config);
    }

    @Test(expected = MissingParameterException.class)
    public void shouldFailIfCoreIsMissing() throws MissingParameterException {
        Map<String, Object> config = createValidConfig();
        config.remove("cores");

        vmLoader.loadVM(config);
    }

    @Test(expected = MissingParameterException.class)
    public void shouldFailIfCacheIsMissing() throws MissingParameterException {
        Map<String, Object> config = createValidConfig();
        config.remove("cacheSize");

        vmLoader.loadVM(config);
    }
}
