package cws.core.algorithms;

import junit.framework.Assert;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.junit.Before;
import org.junit.Test;

import cws.core.simulation.Simulation;

public class SimulationCliTest {
    private Options options;

    @Before
    public void setUp() {
        options = Simulation.buildOptions();
    }

    @Test
    public void testParseAllParams() throws ParseException {
        CommandLineParser parser = new PosixParser();
        String[] args = ("--application GENOME --input-dir /home/xxxxx"
                + " --output-file simulation_out.csv --distribution pareto_unsorted "
                + "--algorithm SPSS --storage-manager global -es 10 --sc fifo").split("\\s+");
        CommandLine cmd = parser.parse(options, args);
        Assert.assertEquals("GENOME", cmd.getOptionValue("app"));
    }
}
