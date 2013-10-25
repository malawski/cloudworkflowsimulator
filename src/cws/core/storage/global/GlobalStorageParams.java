package cws.core.storage.global;

import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import cws.core.exception.IllegalCWSArgumentException;

/**
 * Class containing all parameters for {@link GlobalStorageManager}
 */
public class GlobalStorageParams {
    private static final int DEFAULT_NUM_REPLICAS = 1;

    private static final double DEFAULT_LATENCY = 0.01;

    private static final double DEFAULT_CHUNK_TRANSFER_TIME = 1;

    /** Average read speed of the storage */
    private double readSpeed;

    /** Average write speed of the storage */
    private double writeSpeed;

    /** Average latency for each operation */
    private double latency = DEFAULT_LATENCY;

    /** Number of file system replicas */
    private int numReplicas = DEFAULT_NUM_REPLICAS;

    /**
     * Amount of time spent on transferring one chunk of a file. Should be relatively small, but not too small because
     * we might face some significant floating point arithmetic errors.
     */
    private double chunkTransferTime = DEFAULT_CHUNK_TRANSFER_TIME;

    public static void buildCliOptions(Options options) {
        Option storageManagerRead = new Option(null, "storage-manager-read", true,
                "(required for storage-manager=global) Global storage manager read speed");
        storageManagerRead.setArgName("SPEED");
        options.addOption(storageManagerRead);

        Option storageManagerWrite = new Option(null, "storage-manager-write", true,
                "(required for storage-manager=global) Global storage manager write speed");
        storageManagerWrite.setArgName("SPEED");
        options.addOption(storageManagerWrite);

        Option numReplicas = new Option(null, "num-replicas", true, "Global storage num replicas, defaults to "
                + DEFAULT_NUM_REPLICAS);
        numReplicas.setArgName("NUM");
        options.addOption(numReplicas);

        Option latency = new Option(null, "latency", true, "Global storage latency, defaults to " + DEFAULT_LATENCY);
        latency.setArgName("LATENCY");
        options.addOption(latency);

        Option ctt = new Option(null, "chunk-transfer-time", true,
                "Global storage file chunk transfer time, defaults to " + DEFAULT_CHUNK_TRANSFER_TIME);
        ctt.setArgName("TIME");
        options.addOption(ctt);
    }

    public void storeProperties(Properties properties) {
        properties.setProperty("readSpeed", "" + readSpeed);
        properties.setProperty("writeSpeed", "" + writeSpeed);
        properties.setProperty("chunkTransferTime", "" + chunkTransferTime);
        properties.setProperty("latency", "" + latency);
        properties.setProperty("numReplicas", "" + numReplicas);
    }

    public static GlobalStorageParams readProperties(Properties properties) {
        GlobalStorageParams params = new GlobalStorageParams();
        params.readSpeed = Double.valueOf(properties.getProperty("readSpeed"));
        params.writeSpeed = Double.valueOf(properties.getProperty("writeSpeed"));
        params.chunkTransferTime = Double.valueOf(properties.getProperty("chunkTransferTime",
                DEFAULT_CHUNK_TRANSFER_TIME + ""));
        params.latency = Double.valueOf(properties.getProperty("latency", DEFAULT_LATENCY + ""));
        params.numReplicas = Integer.valueOf(properties.getProperty("numReplicas", DEFAULT_NUM_REPLICAS + ""));
        return params;
    }

    public static GlobalStorageParams readCliOptions(CommandLine args) {
        GlobalStorageParams params = new GlobalStorageParams();
        if (!args.hasOption("storage-manager-read") || !args.hasOption("storage-manager-write")) {
            throw new IllegalCWSArgumentException(
                    "storage-manager-read and storage-manager-read required for GlobalStorageManager");
        }
        params.readSpeed = Double.parseDouble(args.getOptionValue("storage-manager-read"));
        params.writeSpeed = Double.parseDouble(args.getOptionValue("storage-manager-write"));
        params.chunkTransferTime = Double.parseDouble(args.getOptionValue("chunk-transfer-time",
                DEFAULT_CHUNK_TRANSFER_TIME + ""));
        params.latency = Double.parseDouble(args.getOptionValue("latency", DEFAULT_LATENCY + ""));
        params.numReplicas = Integer.parseInt(args.getOptionValue("num-replicas", DEFAULT_NUM_REPLICAS + ""));

        System.out.printf("storage-manager-read = %f\n", params.readSpeed);
        System.out.printf("storage-manager-write = %f\n", params.writeSpeed);
        System.out.printf("latency = %f\n", params.latency);
        System.out.printf("chunk-transfer-time = %f\n", params.chunkTransferTime);
        System.out.printf("num-replicas = %d\n", params.numReplicas);
        return params;
    }

    public double getReadSpeed() {
        return readSpeed;
    }

    public void setReadSpeed(double readSpeed) {
        this.readSpeed = readSpeed;
    }

    public double getWriteSpeed() {
        return writeSpeed;
    }

    public void setWriteSpeed(double writeSpeed) {
        this.writeSpeed = writeSpeed;
    }

    public double getLatency() {
        return latency;
    }

    public void setLatency(double latency) {
        this.latency = latency;
    }

    public double getChunkTransferTime() {
        return chunkTransferTime;
    }

    public void setChunkTransferTime(double chunkTransferTime) {
        this.chunkTransferTime = chunkTransferTime;
    }

    public int getNumReplicas() {
        return numReplicas;
    }

    public void setNumReplicas(int numReplicas) {
        if (numReplicas < 1) {
            throw new IllegalCWSArgumentException("Num replicas must be >= 1");
        }
        this.numReplicas = numReplicas;
    }
}
