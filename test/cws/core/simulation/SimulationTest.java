package cws.core.simulation;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.junit.Test;

import cws.core.exception.IllegalCWSArgumentException;

public class SimulationTest {
    @Test(expected = IllegalCWSArgumentException.class)
    public void shouldFailWhenVMArgIsInvalid() throws ParseException {
        Simulation simulation = new Simulation();

        String[] args = { "--application", "GENOME", "--input-dir", "dags/", "--output-file", "out/", "--distribution",
                "pareto_unsorted", "--algorithm", "DPDS", "--storage-manager", "void", "--vm", "invalid.vm.yaml" };

        Options options = Simulation.buildOptions();
        CommandLineParser parser = new PosixParser();
        CommandLine cmd = parser.parse(options, args);

        simulation.runTest(cmd);
    }

}
