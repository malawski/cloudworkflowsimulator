package cws.core.core;

import static junit.framework.Assert.assertEquals;

import java.util.HashSet;
import java.util.Set;

import org.apache.commons.cli.*;
import org.junit.Before;
import org.junit.Test;

import cws.core.exception.IllegalCWSArgumentException;
import cws.core.provisioner.ConstantDistribution;

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

        final Set<VMType> expected = new HashSet<VMType>();
        expected.add(VMTypeBuilder.newBuilder().mips(1).cores(1).price(1.0).billingTimeInSeconds(3600)
                .cacheSize(53687091200L).provisioningTime(new ConstantDistribution(120))
                .deprovisioningTime(new ConstantDistribution(60)).build());
        expected.add(VMTypeBuilder.newBuilder().mips(1).cores(2).price(2.0).billingTimeInSeconds(3600)
                .cacheSize(53687091200L).provisioningTime(new ConstantDistribution(120))
                .deprovisioningTime(new ConstantDistribution(60)).build());

        final Set<VMType> actual = loader.determineVMTypes(cmd);

        assertEquals(expected, actual);
    }

    @Test
    public void shouldLoadCustomVM() throws ParseException {
        CommandLine cmd = parseArgs(new String[] { "--" + VMTypeLoader.VM_TYPE_OPTION_NAME, "../test/test.vm.yaml" });

        final Set<VMType> expected = new HashSet<VMType>();
        expected.add(VMTypeBuilder.newBuilder().mips(10).cores(3).price(3.5).billingTimeInSeconds(600).cacheSize(12345)
                .provisioningTime(new ConstantDistribution(0)).deprovisioningTime(new ConstantDistribution(10))
                .build());
        expected.add(VMTypeBuilder.newBuilder().mips(20).cores(6).price(7.0).billingTimeInSeconds(300).cacheSize(6789)
                .provisioningTime(new ConstantDistribution(0)).deprovisioningTime(new ConstantDistribution(10))
                .build());

        final Set<VMType> actual = loader.determineVMTypes(cmd);

        assertEquals(expected, actual);
    }

    @Test
    public void shouldLoadVMFromCustomPath() throws ParseException {
        CommandLine cmd = parseArgs(new String[] { "--" + VMTypeLoader.VM_CONFIGS_DIRECTORY_OPTION_NAME, "test",
                "--" + VMTypeLoader.VM_TYPE_OPTION_NAME, "test.vm.yaml" });

        final Set<VMType> expected = new HashSet<VMType>();
        expected.add(VMTypeBuilder.newBuilder().mips(10).cores(3).price(3.5).billingTimeInSeconds(600).cacheSize(12345)
                .provisioningTime(new ConstantDistribution(0)).deprovisioningTime(new ConstantDistribution(10))
                .build());
        expected.add(VMTypeBuilder.newBuilder().mips(20).cores(6).price(7.0).billingTimeInSeconds(300).cacheSize(6789)
                .provisioningTime(new ConstantDistribution(0)).deprovisioningTime(new ConstantDistribution(10))
                .build());

        final Set<VMType> actual = loader.determineVMTypes(cmd);

        assertEquals(expected, actual);
    }

    @Test(expected = IllegalCWSArgumentException.class)
    public void shouldFailWhenFilePathIsInvalid() throws ParseException {
        CommandLine cmd = parseArgs(new String[] { "--" + VMTypeLoader.VM_TYPE_OPTION_NAME, "nosuchfile.vm.yaml" });

        loader.determineVMTypes(cmd);
    }

    @Test(expected = IllegalCWSArgumentException.class)
    public void shouldFailWhenConfigIsInvalid() throws ParseException {
        CommandLine cmd = parseArgs(
                new String[] { "--" + VMTypeLoader.VM_TYPE_OPTION_NAME, "../test/invalid.vm.yaml" });

        loader.determineVMTypes(cmd);
    }

}
