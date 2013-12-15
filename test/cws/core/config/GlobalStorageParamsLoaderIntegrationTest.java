package cws.core.config;

import static junit.framework.Assert.assertEquals;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.junit.Before;
import org.junit.Test;

import cws.core.exception.IllegalCWSArgumentException;
import cws.core.storage.global.GlobalStorageParams;

public class GlobalStorageParamsLoaderIntegrationTest {
    private GlobalStorageParamsLoader loader;

    private CommandLine parseArgs(String[] args) throws ParseException {
        Options options = new Options();
        GlobalStorageParamsLoader.buildCliOptions(options);
        CommandLineParser parser = new PosixParser();
        return parser.parse(options, args);
    }

    @Before
    public void setUp() throws Exception {
        loader = new GlobalStorageParamsLoader();
    }

    @Test
    public void shouldLoadDefaultVM() throws ParseException {
        CommandLine cmd = parseArgs(new String[] {});

        GlobalStorageParams globalStorageParams = loader.determineGlobalStorageParams(cmd);

        assertEquals(30000000.0, globalStorageParams.getReadSpeed());
        assertEquals(10000000.0, globalStorageParams.getWriteSpeed());
        assertEquals(0.01, globalStorageParams.getLatency());
        assertEquals(1, globalStorageParams.getNumReplicas());
        assertEquals(1.0, globalStorageParams.getChunkTransferTime());
    }

    @Test(expected = IllegalCWSArgumentException.class)
    public void shouldFailWhenConfigIsInvalid() throws ParseException {
        CommandLine cmd = parseArgs(new String[] { "--" + GlobalStorageParamsLoader.GS_TYPE_OPTION_NAME,
                "../test/invalid.gs.yaml" });

        loader.determineGlobalStorageParams(cmd);
    }
}
