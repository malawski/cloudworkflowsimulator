package cws.core.transfer;

/**
 * A network port with fixed available bandwidth. This object represents
 * the incoming or outgoing bandwidth of a full-duplex network interface.
 * 
 * The only reason for this object is that we need to keep track of the
 * unique ports so that if multiple transfers make use of a single port we
 * can assign a max-min fair share of the bandwidth to each transfer.
 * 
 * @author Gideon Juve <juve@usc.edu>
 */
public class Port {
    /** Next unique port ID */
    private static int next_id = 0;

    /** Unique ID for this port */
    private int id;

    /** Available bandwidth in Mbps */
    private double bandwidth;

    public Port(double bandwidth) {
        this.id = next_id++;
        this.bandwidth = bandwidth;
    }

    public double getBandwidth() {
        return bandwidth;
    }

    public int getID() {
        return id;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + id;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Port other = (Port) obj;
        if (id != other.id)
            return false;
        return true;
    }
}
