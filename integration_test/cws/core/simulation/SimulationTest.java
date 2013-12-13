package cws.core.simulation;

import static org.mockito.Mockito.when;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import cws.core.core.VMType;
import cws.core.core.VMTypeBuilder;
import cws.core.core.VMTypeLoader;
import cws.core.exception.IllegalCWSArgumentException;

@RunWith(MockitoJUnitRunner.class)
public class SimulationTest {

    @Mock
    VMTypeLoader loader;
    @InjectMocks
    Simulation simulation;

    private CommandLine parsedArgs;

    @Before
    public void setUp() throws Exception {
        String[] validArgs = { "--application", "GENOME", "--input-dir", "dags/", "--output-file",
                "testSimulationTest", "--distribution", "fixed1000", "--algorithm", "DPDS", "--storage-manager",
                "void", "--ensemble-size", "1" };

        Options options = Simulation.buildOptions();
        CommandLineParser parser = new PosixParser();
        parsedArgs = parser.parse(options, validArgs);
    }

    @Test(expected = IllegalCWSArgumentException.class)
    public void shouldFailWhenVMArgIsInvalid() throws ParseException {
        when(loader.determineVMType(parsedArgs)).thenThrow(new IllegalCWSArgumentException("invalid VMType"));

        simulation.runTest(parsedArgs);
    }

    @Test
    public void shouldLoadVMType() throws ParseException {
        VMType vmType = VMTypeBuilder.newBuilder().mips(1).cores(1).price(12.0).build();
        when(loader.determineVMType(parsedArgs)).thenReturn(vmType);

        Simulation simulation = new Simulation(loader);
        simulation.runTest(parsedArgs);
        // smoke test, only checking if no exception was thrown
    }
}
