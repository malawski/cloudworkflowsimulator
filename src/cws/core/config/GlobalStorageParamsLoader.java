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

    static final String GS_READ_SPEED_OPTION_NAME = "gs-read-speed";
    static final String GS_READ_SPEED_SHORT_OPTION_NAME = "gsrs";
    static final String GS_WRITE_SPEED_OPTION_NAME = "gs-write-speed";
    static final String GS_WRITE_SPEED_SHORT_OPTION_NAME = "gsws";
    static final String GS_LATENCY_OPTION_NAME = "gs-latency";
    static final String GS_LATENCY_SHORT_OPTION_NAME = "gsl";
    static final String GS_CHUNK_TRANSFER_TIME_OPTION_NAME = "gs-chunk-time";
    static final String GS_CHUNK_TRANSFER_TIME_SHORT_OPTION_NAME = "gsct";
    static final String GS_REPLICAS_NUMBER_OPTION_NAME = "gs-replicas";
    static final String GS_REPLICAS_NUMBER_SHORT_OPTION_NAME = "gsr";

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

        Option readSpeed = new Option(GS_READ_SPEED_SHORT_OPTION_NAME, GS_READ_SPEED_OPTION_NAME, HAS_ARG,
                "Overrides Global Storage read speed");
        readSpeed.setArgName("BYTES/SEC");
        options.addOption(readSpeed);

        Option writeSpeed = new Option(GS_WRITE_SPEED_SHORT_OPTION_NAME, GS_WRITE_SPEED_OPTION_NAME, HAS_ARG,
                "Overrides Global Storage write speed");
        writeSpeed.setArgName("BYTES/SEC");
        options.addOption(writeSpeed);

        Option replicasNumber = new Option(GS_REPLICAS_NUMBER_SHORT_OPTION_NAME, GS_REPLICAS_NUMBER_OPTION_NAME,
                HAS_ARG, "Overrides Global Storage replicas number");
        replicasNumber.setArgName("NUM");
        options.addOption(replicasNumber);

        Option latency = new Option(GS_LATENCY_SHORT_OPTION_NAME, GS_LATENCY_OPTION_NAME, HAS_ARG,
                "Overrides Global Storage latency");
        latency.setArgName("SECONDS");
        options.addOption(latency);

        Option chunkTransferTime = new Option(GS_CHUNK_TRANSFER_TIME_SHORT_OPTION_NAME,
                GS_CHUNK_TRANSFER_TIME_OPTION_NAME, HAS_ARG, "Overrides Global Storage transfer time");
        chunkTransferTime.setArgName("SECONDS");
        options.addOption(chunkTransferTime);
    }

    public GlobalStorageParams determineGlobalStorageParams(CommandLine args) throws IllegalCWSArgumentException {
        Map<String, Object> globalStorageConfig = tryLoadConfigFromFile(args);
        overrideConfigFromFileWithCliArgs(globalStorageConfig, args);
        return loadParams(globalStorageConfig);
    }

    void overrideConfigFromFileWithCliArgs(Map<String, Object> globalStorageConfig, CommandLine args) {
        overrideReadSpeed(globalStorageConfig, args);
        overrideWriteSpeed(globalStorageConfig, args);
        overrideLatency(globalStorageConfig, args);
        overrideChunkTransferTime(globalStorageConfig, args);
        overrideReplicasNumber(globalStorageConfig, args);
    }

    private void overrideReplicasNumber(Map<String, Object> globalStorageConfig, CommandLine args) {
        if (args.hasOption(GS_REPLICAS_NUMBER_OPTION_NAME)) {
            try {
                Integer replicasNumber = Integer.parseInt(args.getOptionValue(GS_REPLICAS_NUMBER_OPTION_NAME));
                globalStorageConfig.put(GS_REPLICAS_NUMBER_CONFIG_ENTRY, replicasNumber);
            } catch (NumberFormatException e) {
                throw new IllegalCWSArgumentException(GS_REPLICAS_NUMBER_CONFIG_ENTRY
                        + " was overrode with a non-integer value");
            }
        }
    }

    private void overrideChunkTransferTime(Map<String, Object> globalStorageConfig, CommandLine args) {
        if (args.hasOption(GS_CHUNK_TRANSFER_TIME_OPTION_NAME)) {
            try {
                Double chunkTransferTime = Double.parseDouble(args.getOptionValue(GS_CHUNK_TRANSFER_TIME_OPTION_NAME));
                globalStorageConfig.put(GS_CHUNK_TRANSFER_TIME_CONFIG_ENTRY, chunkTransferTime);
            } catch (NumberFormatException e) {
                throw new IllegalCWSArgumentException(GS_CHUNK_TRANSFER_TIME_CONFIG_ENTRY
                        + " was overrode with a non-number value");
            }
        }
    }

    private void overrideLatency(Map<String, Object> globalStorageConfig, CommandLine args) {
        if (args.hasOption(GS_LATENCY_OPTION_NAME)) {
            try {
                Double latency = Double.parseDouble(args.getOptionValue(GS_LATENCY_OPTION_NAME));
                globalStorageConfig.put(GS_LATENCY_CONFIG_ENTRY, latency);
            } catch (NumberFormatException e) {
                throw new IllegalCWSArgumentException(GS_LATENCY_CONFIG_ENTRY + " was overrode with a non-number value");
            }
        }
    }

    private void overrideWriteSpeed(Map<String, Object> globalStorageConfig, CommandLine args) {
        if (args.hasOption(GS_WRITE_SPEED_OPTION_NAME)) {
            try {
                Double writeSpeed = Double.parseDouble(args.getOptionValue(GS_WRITE_SPEED_OPTION_NAME));
                globalStorageConfig.put(GS_WRITE_SPEED_CONFIG_ENTRY, writeSpeed);
            } catch (NumberFormatException e) {
                throw new IllegalCWSArgumentException(GS_WRITE_SPEED_CONFIG_ENTRY
                        + " was overrode with a non-number value");
            }
        }
    }

    private void overrideReadSpeed(Map<String, Object> globalStorageConfig, CommandLine args) {
        if (args.hasOption(GS_READ_SPEED_OPTION_NAME)) {
            try {
                Double readSpeed = Double.parseDouble(args.getOptionValue(GS_READ_SPEED_OPTION_NAME));
                globalStorageConfig.put(GS_READ_SPEED_CONFIG_ENTRY, readSpeed);
            } catch (NumberFormatException e) {
                throw new IllegalCWSArgumentException(GS_READ_SPEED_CONFIG_ENTRY
                        + " was overrode with a non-number value");
            }
        }
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
        assertIsInteger(config, GS_REPLICAS_NUMBER_CONFIG_ENTRY);
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

    private void assertIsInteger(Map<String, Object> config, String configEntry) {
        if (!(config.get(configEntry) instanceof Integer)) {
            throw new IllegalCWSArgumentException(configEntry + " configuration is not an integer number");
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
