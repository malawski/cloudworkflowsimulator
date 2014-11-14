package cws.core.simulation;

import static org.mockito.Mockito.when;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import cws.core.config.CommandLineBuilder;
import cws.core.config.GlobalStorageParamsLoader;
import cws.core.core.VMType;
import cws.core.core.VMTypeBuilder;
import cws.core.core.VMTypeLoader;
import cws.core.exception.IllegalCWSArgumentException;
import cws.core.storage.global.GlobalStorageParams;

@RunWith(MockitoJUnitRunner.class)
public class SimulationTest {

    @Mock
    VMTypeLoader vmTypeLoader;

    @Mock
    GlobalStorageParamsLoader globalStorageParamsLoader;

    @InjectMocks
    Simulation simulation;

    private CommandLineBuilder validArgs;

    @Before
    public void setUp() throws Exception {
        Options options = Simulation.buildOptions();
        validArgs = CommandLineBuilder.fromOptions(options).addOption("application", "GENOME")
                .addOption("input-dir", "dags/").addOption("output-file", "testSimulationTest")
                .addOption("distribution", "fixed1000").addOption("algorithm", "DPDS").addOption("ensemble-size", "1");
    }

    private void mockLoadersValidReturnTypes(CommandLine args) {
        VMType vmType = VMTypeBuilder.newBuilder().mips(1.0).cores(1).price(12.0).build();
        when(vmTypeLoader.determineVMType(args)).thenReturn(vmType);

        GlobalStorageParams globalStorageParams = new GlobalStorageParams();
        when(globalStorageParamsLoader.determineGlobalStorageParams(args)).thenReturn(globalStorageParams);
    }

    @Test
    public void shouldLoadVMTypeAndGSType() throws ParseException {
        CommandLine args = validArgs.addOption("storage-manager", "void").build();

        mockLoadersValidReturnTypes(args);

        // smoke test, only checking if no exception was thrown
        simulation.runTest(args);
    }

    @Test(expected = IllegalCWSArgumentException.class)
    public void shouldFailWhenVMArgIsInvalid() throws ParseException {
        CommandLine args = validArgs.addOption("storage-manager", "global").build();

        mockLoadersValidReturnTypes(args);
        when(vmTypeLoader.determineVMType(args)).thenThrow(new IllegalCWSArgumentException("invalid VMType"));

        simulation.runTest(args);
    }

    public void shouldNotRequireGlobalStorageParamsWithVoidStorage() throws ParseException {
        CommandLine args = validArgs.addOption("storage-manager", "void").build();

        mockLoadersValidReturnTypes(args);
        when(globalStorageParamsLoader.determineGlobalStorageParams(args)).thenThrow(
                new IllegalCWSArgumentException("invalid GSParams"));

        // smoke test, only checking if no exception was thrown
        simulation.runTest(args);
    }

    @Test(expected = IllegalCWSArgumentException.class)
    public void shouldRequireGlobalStorageParamsWhenGlobalStorageIsChosen() throws ParseException {
        CommandLine args = validArgs.addOption("storage-manager", "global").build();

        mockLoadersValidReturnTypes(args);
        when(globalStorageParamsLoader.determineGlobalStorageParams(args)).thenThrow(
                new IllegalCWSArgumentException("invalid GSParams"));

        simulation.runTest(args);
    }

}
