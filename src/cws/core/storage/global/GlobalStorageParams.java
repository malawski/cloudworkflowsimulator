package cws.core.storage.global;

/**
 * TODO(bryk): we could read those parameters from a .properties file.
 * Class containing all parameters for {@link GlobalStorageManager}
 */
public class GlobalStorageParams {
    /** Average read speed of the storage */
    private double readSpeed;

    /** Average write speed of the storage */
    private double writeSpeed;

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
}
