package cws.core.storage.global;

/**
 * TODO(bryk):
 */
public class CongestedGlobalStorageParams {
    private double readSpeed;
    private double writeSpeed;
    private int numReads;
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
        numWrites -= i;
    }

    public void removeReads(int i) {
        numReads -= i;
    }
}
