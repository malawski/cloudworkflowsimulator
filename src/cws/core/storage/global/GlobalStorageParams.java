package cws.core.storage.global;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

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

    /**
     * @return Properties file name prefix based on this prams' state.
     */
    public String getName() {
        return "rs_" + readSpeed + "ws_" + writeSpeed + "ctt_" + chunkTransferTime + "l_" + latency + "nr_"
                + numReplicas;
    }

    /**
     * @return All global storage params permutations.
     */
    public static List<GlobalStorageParams> getAllGlobalStorageParams() {
        List<GlobalStorageParams> ret = new ArrayList<GlobalStorageParams>();
        for (double ws = 10000; ws <= 100000; ws *= 10) {
            for (double rs = 10000; rs <= 100000; rs *= 10) {
                GlobalStorageParams gs = new GlobalStorageParams();
                gs.setReadSpeed(rs);
                gs.setWriteSpeed(ws);
                ret.add(gs);
            }
        }
        return ret;
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
