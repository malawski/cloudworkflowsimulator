package cws.core.core;

import static junit.framework.Assert.assertEquals;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.junit.Before;
import org.junit.Test;

import cws.core.exception.IllegalCWSArgumentException;

public class VMTypeLoaderIntegrationTest {

    private VMTypeLoader loader;

    private CommandLine parseArgs(String[] args) throws ParseException {
        Options options = new Options();
        VMTypeLoader.buildCliOptions(options);
        CommandLineParser parser = new PosixParser();
        return parser.parse(options, args);
    }

    @Before
    public void setUp() throws Exception {
        loader = new VMTypeLoader();
    }

    @Test
    public void shouldLoadDefaultVM() throws ParseException {
        CommandLine cmd = parseArgs(new String[] {});

        VMType vmType = loader.determineVMType(cmd);

        assertEquals(1, vmType.getMips());
        assertEquals(1, vmType.getCores());
        assertEquals(3600.0, vmType.getBillingTimeInSeconds());
        assertEquals(1.0, vmType.getPriceForBillingUnit());
    }

    @Test
    public void shouldLoadCustomVM() throws ParseException {
        CommandLine cmd = parseArgs(new String[] { "--" + VMTypeLoader.VM_TYPE_OPTION_NAME, "../test/test.vm.yaml" });

        VMType vmType = loader.determineVMType(cmd);

        assertEquals(10, vmType.getMips());
        assertEquals(3, vmType.getCores());
        assertEquals(600.0, vmType.getBillingTimeInSeconds());
        assertEquals(3.5, vmType.getPriceForBillingUnit());
        assertEquals(12345L, vmType.getCacheSize());
    }

    @Test(expected = IllegalCWSArgumentException.class)
    public void shouldFailWhenFilePathIsInvalid() throws ParseException {
        CommandLine cmd = parseArgs(new String[] { "--" + VMTypeLoader.VM_TYPE_OPTION_NAME, "nosuchfile.vm.yaml" });

        loader.determineVMType(cmd);
    }

    @Test(expected = IllegalCWSArgumentException.class)
    public void shouldFailWhenConfigIsInvalid() throws ParseException {
        CommandLine cmd = parseArgs(new String[] { "--" + VMTypeLoader.VM_TYPE_OPTION_NAME, "../test/invalid.vm.yaml" });

        loader.determineVMType(cmd);
    }

}
