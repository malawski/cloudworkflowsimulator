package cws.core.algorithms;

import junit.framework.Assert;

import org.apache.commons.cli.*;
import org.junit.Before;
import org.junit.Test;

import cws.core.simulation.Simulation;
import cws.core.storage.global.GlobalStorageParams;

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
                + "--algorithm SPSS --storage-manager global -s100 --storage-manager-read=1000000000 "
                + "--storage-manager-write=30000000 -es 10 --sc fifo").split("\\s+");
        CommandLine cmd = parser.parse(options, args);
        Assert.assertEquals("GENOME", cmd.getOptionValue("app"));
    }

    @Test
    public void testReadGlobalStorageParams() throws ParseException {
        CommandLineParser parser = new PosixParser();
        String[] args = ("--application GENOME --input-dir /home/xxxxx"
                + " --output-file simulation_out.csv --distribution pareto_unsorted "
                + "--algorithm SPSS --storage-manager global -s100 --storage-manager-read=321 "
                + "--storage-manager-write=123 -es 10 --sc fifo" + " --chunk-transfer-time=333" + " --num-replicas=98")
                .split("\\s+");
        CommandLine cmd = parser.parse(options, args);
        GlobalStorageParams params = GlobalStorageParams.readCliOptions(cmd);
        Assert.assertEquals(123, params.getWriteSpeed(), 0.001);
        Assert.assertEquals(321, params.getReadSpeed(), 0.001);
        Assert.assertEquals(333, params.getChunkTransferTime(), 0.001);
        Assert.assertEquals(98, params.getNumReplicas());
    }
}
