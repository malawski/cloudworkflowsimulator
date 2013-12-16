package cws.core.config;

import static junit.framework.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.junit.Before;
import org.junit.Test;

import cws.core.exception.IllegalCWSArgumentException;

public class GlobalStorageParamsLoaderCliTest {
    private GlobalStorageParamsLoader loader;
    private Map<String, Object> config;
    private Options options;

    @Before
    public void setUp() throws Exception {
        loader = new GlobalStorageParamsLoader();
        createValidConfig();

        options = new Options();
        GlobalStorageParamsLoader.buildCliOptions(options);
    }

    private void createValidConfig() {
        config = new HashMap<String, Object>();
        config.put(GlobalStorageParamsLoader.GS_READ_SPEED_CONFIG_ENTRY, 1.0);
        config.put(GlobalStorageParamsLoader.GS_WRITE_SPEED_CONFIG_ENTRY, 1.0);
        config.put(GlobalStorageParamsLoader.GS_LATENCY_CONFIG_ENTRY, 0.01);
        config.put(GlobalStorageParamsLoader.GS_REPLICAS_NUMBER_CONFIG_ENTRY, 1);
        config.put(GlobalStorageParamsLoader.GS_CHUNK_TRANSFER_TIME_CONFIG_ENTRY, 3.2);
    }

    @Test
    public void shouldEnableToOverrideReadSpeed() throws ParseException {
        CommandLine args = CommandLineBuilder.fromOptions(options)
                .addOption(GlobalStorageParamsLoader.GS_READ_SPEED_OPTION_NAME, "12345.5").build();

        loader.overrideConfigFromFileWithCliArgs(config, args);

        assertEquals(12345.5, config.get(GlobalStorageParamsLoader.GS_READ_SPEED_CONFIG_ENTRY));
    }

    @Test
    public void shouldEnableToOverrideReadSpeedWithShortOption() throws ParseException {
        CommandLine args = CommandLineBuilder.fromOptions(options)
                .addShortOption(GlobalStorageParamsLoader.GS_READ_SPEED_SHORT_OPTION_NAME, "12345.5").build();

        loader.overrideConfigFromFileWithCliArgs(config, args);

        assertEquals(12345.5, config.get(GlobalStorageParamsLoader.GS_READ_SPEED_CONFIG_ENTRY));
    }

    @Test(expected = IllegalCWSArgumentException.class)
    public void shouldFailIfOverrideReadSpeedWithInvalidValue() throws ParseException {
        CommandLine args = CommandLineBuilder.fromOptions(options)
                .addOption(GlobalStorageParamsLoader.GS_READ_SPEED_OPTION_NAME, "invalid").build();

        loader.overrideConfigFromFileWithCliArgs(config, args);
    }

    @Test
    public void shouldEnableToOverrideWriteSpeed() throws ParseException {
        CommandLine args = CommandLineBuilder.fromOptions(options)
                .addOption(GlobalStorageParamsLoader.GS_WRITE_SPEED_OPTION_NAME, "432.5").build();

        loader.overrideConfigFromFileWithCliArgs(config, args);

        assertEquals(432.5, config.get(GlobalStorageParamsLoader.GS_WRITE_SPEED_CONFIG_ENTRY));
    }

    @Test
    public void shouldEnableToOverrideWriteSpeedWithShortOption() throws ParseException {
        CommandLine args = CommandLineBuilder.fromOptions(options)
                .addOption(GlobalStorageParamsLoader.GS_WRITE_SPEED_SHORT_OPTION_NAME, "432.5").build();

        loader.overrideConfigFromFileWithCliArgs(config, args);

        assertEquals(432.5, config.get(GlobalStorageParamsLoader.GS_WRITE_SPEED_CONFIG_ENTRY));
    }

    @Test(expected = IllegalCWSArgumentException.class)
    public void shouldFailIfOverrideWriteSpeedWithInvalidValue() throws ParseException {
        CommandLine args = CommandLineBuilder.fromOptions(options)
                .addOption(GlobalStorageParamsLoader.GS_WRITE_SPEED_OPTION_NAME, "invalid").build();

        loader.overrideConfigFromFileWithCliArgs(config, args);
    }

    @Test
    public void shouldEnableToOverrideLatency() throws ParseException {
        CommandLine args = CommandLineBuilder.fromOptions(options)
                .addOption(GlobalStorageParamsLoader.GS_LATENCY_OPTION_NAME, "0.123").build();

        loader.overrideConfigFromFileWithCliArgs(config, args);

        assertEquals(0.123, config.get(GlobalStorageParamsLoader.GS_LATENCY_CONFIG_ENTRY));
    }

    @Test
    public void shouldEnableToOverrideLatencyWithShortOption() throws ParseException {
        CommandLine args = CommandLineBuilder.fromOptions(options)
                .addOption(GlobalStorageParamsLoader.GS_LATENCY_SHORT_OPTION_NAME, "0.123").build();

        loader.overrideConfigFromFileWithCliArgs(config, args);

        assertEquals(0.123, config.get(GlobalStorageParamsLoader.GS_LATENCY_CONFIG_ENTRY));
    }

    @Test(expected = IllegalCWSArgumentException.class)
    public void shouldFailIfOverrideLatencyWithInvalidValue() throws ParseException {
        CommandLine args = CommandLineBuilder.fromOptions(options)
                .addOption(GlobalStorageParamsLoader.GS_LATENCY_OPTION_NAME, "invalid").build();

        loader.overrideConfigFromFileWithCliArgs(config, args);
    }

    @Test
    public void shouldEnableToOverrideChunkTransferTime() throws ParseException {
        CommandLine args = CommandLineBuilder.fromOptions(options)
                .addOption(GlobalStorageParamsLoader.GS_CHUNK_TRANSFER_TIME_OPTION_NAME, "0.5").build();

        loader.overrideConfigFromFileWithCliArgs(config, args);

        assertEquals(0.5, config.get(GlobalStorageParamsLoader.GS_CHUNK_TRANSFER_TIME_CONFIG_ENTRY));
    }

    @Test
    public void shouldEnableToOverrideChunkTransferTimeWithShortOption() throws ParseException {
        CommandLine args = CommandLineBuilder.fromOptions(options)
                .addOption(GlobalStorageParamsLoader.GS_CHUNK_TRANSFER_TIME_SHORT_OPTION_NAME, "0.5").build();

        loader.overrideConfigFromFileWithCliArgs(config, args);

        assertEquals(0.5, config.get(GlobalStorageParamsLoader.GS_CHUNK_TRANSFER_TIME_CONFIG_ENTRY));
    }

    @Test(expected = IllegalCWSArgumentException.class)
    public void shouldFailIfOverrideChunkTransferTimeWithInvalidValue() throws ParseException {
        CommandLine args = CommandLineBuilder.fromOptions(options)
                .addOption(GlobalStorageParamsLoader.GS_CHUNK_TRANSFER_TIME_OPTION_NAME, "invalid").build();

        loader.overrideConfigFromFileWithCliArgs(config, args);
    }

    @Test
    public void shouldEnableToOverrideReplicasNumber() throws ParseException {
        CommandLine args = CommandLineBuilder.fromOptions(options)
                .addOption(GlobalStorageParamsLoader.GS_REPLICAS_NUMBER_OPTION_NAME, "123").build();

        loader.overrideConfigFromFileWithCliArgs(config, args);

        assertEquals(123, config.get(GlobalStorageParamsLoader.GS_REPLICAS_NUMBER_CONFIG_ENTRY));
    }

    @Test
    public void shouldEnableToOverrideReplicasNumberWithShortOption() throws ParseException {
        CommandLine args = CommandLineBuilder.fromOptions(options)
                .addOption(GlobalStorageParamsLoader.GS_REPLICAS_NUMBER_SHORT_OPTION_NAME, "123").build();

        loader.overrideConfigFromFileWithCliArgs(config, args);

        assertEquals(123, config.get(GlobalStorageParamsLoader.GS_REPLICAS_NUMBER_CONFIG_ENTRY));
    }

    @Test(expected = IllegalCWSArgumentException.class)
    public void shouldFailIfOverrideReplicasNumberWithInvalidValue() throws ParseException {
        CommandLine args = CommandLineBuilder.fromOptions(options)
                .addOption(GlobalStorageParamsLoader.GS_REPLICAS_NUMBER_OPTION_NAME, "2.3").build();

        loader.overrideConfigFromFileWithCliArgs(config, args);
    }

}
