package cws.core.config;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.yaml.snakeyaml.Yaml;

import cws.core.exception.IllegalCWSArgumentException;
import cws.core.storage.global.GlobalStorageParams;

/**
 * Loads GlobalStorageParams from *.gs.yaml config file.
 * 
 * Uses --global-storage filename option. When the option is not specified
 * loads gs/default.gs.yaml file. Global storage files paths should be
 * specified relatively to gs/ directory by default.
 * 
 * Global storage params can be overrode by CLI args like --gs-read-speed.
 */

public class GlobalStorageParamsLoader {
    static final String GS_TYPE_SHORT_OPTION_NAME = "gs";
    static final String GS_TYPE_OPTION_NAME = "global-storage";
    private static final String DEFAULT_GS_TYPE_FILENAME = "default.gs.yaml";
    private static final boolean HAS_ARG = true;
    static final String GS_READ_SPEED_CONFIG_ENTRY = "readSpeed";
    static final String GS_WRITE_SPEED_CONFIG_ENTRY = "writeSpeed";
    static final String GS_LATENCY_CONFIG_ENTRY = "latency";
    static final String GS_CHUNK_TRANSFER_TIME_CONFIG_ENTRY = "chunkTransferTime";
    static final String GS_REPLICAS_NUMBER_CONFIG_ENTRY = "replicas";
    static final String GS_CONFIGS_DIRECTORY_OPTION_NAME = "global-storage-directory";
    static final String GS_CONFIGS_DIRECTORY_SHORT_OPTION_NAME = "gsd";
    private static final String DEFAULT_GS_CONFIGS_DIRECTORY = "gs/";

    public static void buildCliOptions(Options options) {
        Option globalStorage = new Option(GS_TYPE_SHORT_OPTION_NAME, GS_TYPE_OPTION_NAME, HAS_ARG, String.format(
                "Global storage config filename, defaults to %s", DEFAULT_GS_TYPE_FILENAME));
        globalStorage.setArgName("FILENAME");
        options.addOption(globalStorage);

        Option configsDirectory = new Option(
                GS_CONFIGS_DIRECTORY_SHORT_OPTION_NAME,
                GS_CONFIGS_DIRECTORY_OPTION_NAME,
                HAS_ARG,
                String.format(
                        "Global storage config directory, config files are loaded relatively to its path, defaults to %s",
                        DEFAULT_GS_CONFIGS_DIRECTORY));
        configsDirectory.setArgName("DIRPATH");
        options.addOption(configsDirectory);
    }

    public GlobalStorageParams determineGlobalStorageParams(CommandLine args) throws IllegalCWSArgumentException {
        Map<String, Object> globalStorageConfig = tryLoadConfigFromFile(args);
        return loadParams(globalStorageConfig);
    }

    private Map<String, Object> tryLoadConfigFromFile(CommandLine args) {
        try {
            return loadConfigFromFile(args);
        } catch (FileNotFoundException e) {
            throw new IllegalCWSArgumentException("Cannot load Global Storage config file: " + e.getMessage());
        }
    }

    private Map<String, Object> loadConfigFromFile(CommandLine args) throws FileNotFoundException {
        String gsConfigFilename = args.getOptionValue(GS_TYPE_OPTION_NAME, DEFAULT_GS_TYPE_FILENAME);
        String gsConfigDirectory = args.getOptionValue(GS_CONFIGS_DIRECTORY_OPTION_NAME, DEFAULT_GS_CONFIGS_DIRECTORY);

        InputStream input = new FileInputStream(new File(gsConfigDirectory, gsConfigFilename));
        Yaml yaml = new Yaml();
        return (Map<String, Object>) yaml.load(input);
    }

    public GlobalStorageParams loadParams(Map<String, Object> config) throws IllegalCWSArgumentException {
        double readSpeed = loadReadSpeed(config);
        double writeSpeed = loadWriteSpeed(config);
        double latency = loadLatency(config);
        double chunkTransferTime = loadChunkTransferTime(config);
        int replicasNumber = loadReplicasNumber(config);

        // TODO(mequrel): convert into builder
        GlobalStorageParams params = new GlobalStorageParams();
        params.setReadSpeed(readSpeed);
        params.setWriteSpeed(writeSpeed);
        params.setLatency(latency);
        params.setChunkTransferTime(chunkTransferTime);
        params.setNumReplicas(replicasNumber);

        return params;
    }

    private int loadReplicasNumber(Map<String, Object> config) {
        assertRequiredOptionIsNotMissing(config, GS_REPLICAS_NUMBER_CONFIG_ENTRY);
        assertIsNumber(config, GS_REPLICAS_NUMBER_CONFIG_ENTRY);
        int replicasNumber = toInt(config, GS_REPLICAS_NUMBER_CONFIG_ENTRY);
        assertIsGreaterThanZero(replicasNumber, GS_REPLICAS_NUMBER_CONFIG_ENTRY);
        return replicasNumber;
    }

    private double loadChunkTransferTime(Map<String, Object> config) {
        assertRequiredOptionIsNotMissing(config, GS_CHUNK_TRANSFER_TIME_CONFIG_ENTRY);
        assertIsNumber(config, GS_CHUNK_TRANSFER_TIME_CONFIG_ENTRY);
        double chunkTransferTime = toDouble(config, GS_CHUNK_TRANSFER_TIME_CONFIG_ENTRY);
        assertIsGreaterThanZero(chunkTransferTime, GS_CHUNK_TRANSFER_TIME_CONFIG_ENTRY);
        return chunkTransferTime;
    }

    private double loadLatency(Map<String, Object> config) {
        assertRequiredOptionIsNotMissing(config, GS_LATENCY_CONFIG_ENTRY);
        assertIsNumber(config, GS_LATENCY_CONFIG_ENTRY);
        double latency = toDouble(config, GS_LATENCY_CONFIG_ENTRY);
        assertIsGreaterOrEqualZero(GS_LATENCY_CONFIG_ENTRY, latency);
        return latency;
    }

    private double loadWriteSpeed(Map<String, Object> config) {
        assertRequiredOptionIsNotMissing(config, GS_WRITE_SPEED_CONFIG_ENTRY);
        assertIsNumber(config, GS_WRITE_SPEED_CONFIG_ENTRY);
        double writeSpeed = toDouble(config, GS_WRITE_SPEED_CONFIG_ENTRY);
        assertIsGreaterThanZero(writeSpeed, GS_WRITE_SPEED_CONFIG_ENTRY);
        return writeSpeed;
    }

    private double loadReadSpeed(Map<String, Object> config) {
        assertRequiredOptionIsNotMissing(config, GS_READ_SPEED_CONFIG_ENTRY);
        assertIsNumber(config, GS_READ_SPEED_CONFIG_ENTRY);
        double readSpeed = toDouble(config, GS_READ_SPEED_CONFIG_ENTRY);
        assertIsGreaterThanZero(readSpeed, GS_READ_SPEED_CONFIG_ENTRY);
        return readSpeed;
    }

    private void assertIsGreaterOrEqualZero(String configEntry, double value) {
        if (lessThanZero(value)) {
            throw new IllegalCWSArgumentException(configEntry + " configuration is less than zero");
        }
    }

    private void assertIsGreaterThanZero(int value, String configEntry) {
        if (value < 1) {
            throw new IllegalCWSArgumentException(configEntry + " configuration is not greater than zero");
        }
    }

    private void assertIsGreaterThanZero(double value, String configEntry) {
        if (lessOrEqualToZero(value)) {
            throw new IllegalCWSArgumentException(configEntry + " configuration is not greater than zero");
        }
    }

    private void assertIsNumber(Map<String, Object> config, String configEntry) {
        if (!(config.get(configEntry) instanceof Number)) {
            throw new IllegalCWSArgumentException(configEntry + " configuration is not a number");
        }
    }

    private void assertRequiredOptionIsNotMissing(Map<String, Object> config, String configEntry) {
        if (!config.containsKey(configEntry)) {
            throw new IllegalCWSArgumentException(configEntry + " configuration is missing in GS config file");
        }
    }

    private double toDouble(Map<String, Object> config, String configEntry) {
        Number value = (Number) config.get(configEntry);
        return value.doubleValue();
    }

    private int toInt(Map<String, Object> config, String configEntry) {
        Number value = (Number) config.get(configEntry);
        return value.intValue();
    }

    private boolean lessThanZero(double number) {
        return Double.compare(number, 0.0) < 0;
    }

    private boolean lessOrEqualToZero(double number) {
        return Double.compare(number, 0.0) < 1;
    }
}
