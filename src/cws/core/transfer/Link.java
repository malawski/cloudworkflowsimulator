package cws.core.transfer;

/**
 * This object represents a link between two network elements. It has a fixed
 * available bandwidth, a latency, and a maximum transmission unit (MTU).
 * 
 * This object exists so that we can simulate latency and congested links. If
 * you want to simulate latency without congestion, then set the bandwidth to
 * Double.MAX_VALUE. If you want congestion without latency, set the latency
 * to 0.
 * 
 * Some typical round-trip times are:
 * 
 * Switch: < 1 ms
 * LAN: < 5 ms
 * LA to SF: 10 ms
 * LA to Boston: 80 ms
 * LA to UK: 150 ms
 * LA to Israel: 250 ms
 * LA to China: 210 ms
 * LA to Australia: 190 ms
 * LA to India: 290 ms
 * 
 * @author Gideon Juve <juve@usc.edu>
 */
public class Link {
    /** Next link ID */
    private static int next_id = 0;

    /** Default MTU for Ethernet is 1500 bytes */
    private static final int DEFAULT_MTU = 1500;

    /** Unique ID for the Link */
    private int id;

    /** Bandwidth of the link in Mbps */
    private double bandwidth;

    /** Round-trip latency of the link in milliseconds */
    private double rtt;

    /** The MTU (maximum transmission unit) of the link in bytes */
    private int mtu;

    /**
     * @param bandwidth Bandwidth of link in Mbps
     * @param rtt Round-trip latency in ms
     * @param mtu MTU of link in bytes
     */
    public Link(double bandwidth, double rtt, int mtu) {
        this.id = next_id++;
        this.bandwidth = bandwidth;
        this.rtt = rtt;
        this.mtu = mtu;
    }

    /**
     * @param bandwidth Bandwidth of link in Mbps
     * @param rtt Round-trip latency in ms
     */
    public Link(double bandwidth, double rtt) {
        this(bandwidth, rtt, DEFAULT_MTU);
    }

    public double getBandwidth() {
        return bandwidth;
    }

    public double getRTT() {
        return rtt;
    }

    public int getMTU() {
        return mtu;
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
        Link other = (Link) obj;
        if (id != other.id)
            return false;
        return true;
    }
}
