package cws.core.storage.global;

/**
 * Parameters used by {@link GlobalStorageManager} to simulate congestion.
 * @see {@link GlobalStorageParams} for reference
 */
public class CongestedGlobalStorageParams {
    /** Congested readSpeed */
    private double readSpeed;
    /** Congested writeSpeed */
    private double writeSpeed;
    /** The number of active read */
    private int numReads;
    /** The number of active writes */
    private int numWrites;

    public CongestedGlobalStorageParams(GlobalStorageParams params) {
        this.readSpeed = params.getReadSpeed();
        this.writeSpeed = params.getWriteSpeed();
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

    public int getNumReads() {
        return numReads;
    }

    public int getNumWrites() {
        return numWrites;
    }

    public void addWrites(int size) {
        numWrites += size;
    }

    public void addReads(int size) {
        numReads += size;
    }

    public void removeWrites(int i) {
        if (numWrites == 0) {
            throw new IllegalStateException("Cannot go under 0 writes");
        }
        numWrites -= i;
    }

    public void removeReads(int i) {
        if (numReads == 0) {
            throw new IllegalStateException("Cannot go under 0 reads");
        }
        numReads -= i;
    }
}
