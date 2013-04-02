package cws.core.transfer;

import org.cloudbus.cloudsim.core.CloudSim;

/**
 * Simulates a data/file transfer from one network port to another over
 * a link. The source and destination ports have a fixed available bandwidth
 * and the link has a fixed available bandwidth, latency, and MTU.
 * 
 * The goal is to efficiently simulate contention at end points and over
 * congested links.
 * 
 * This adds overheads to the transfer size to account for the packet headers.
 * 
 * @author Gideon Juve <juve@usc.edu>
 */
public class Transfer {
    private static long next_id = 0;

    /** Conversion constant for Mbps to bps */
    public static final double MBPS_TO_BPS = 1000000.0;

    /** Bytes of overhead per packet. Based on IPv4 (20B) and TCP (20B) */
    public static final int PACKET_OVERHEAD = 20 + 20;

    /** Unique ID for this transfer */
    private long id;

    /** The entity that initiated the transfer */
    private int owner;

    /** The source port of the transfer */
    private Port src;

    /** The destination port of the transfer */
    private Port dest;

    /** The link over which the transfer occurs */
    private Link link;

    /** Data size of transfer in bytes */
    private long dataSize;

    /** The number of bytes that will be transferred including overheads */
    private long transferSize;

    /** Number of bytes remaining in transfer */
    private long bytesRemaining;

    /** Current bandwidth assigned to transfer in Mbps */
    private double currentBandwidth;

    /**
     * Clock time of last change to bandwidth
     * This is used to calculate how much has been transferred
     * since the last change.
     */
    private double lastUpdate;

    /** Start time of transfer */
    private double startTime;

    /** Finish time of transfer */
    private double finishTime;

    /**
     * Every transfer has a source port, a destination port, a link that the data travels
     * over, and an owner.
     * 
     * @param source Source Port of transfer
     * @param destination Destination Port of transfer
     * @param link The link over which the transfer occurs
     * @param dataSize Size of data transfer in bytes
     * @param owner The entity that owns this transfer
     */
    public Transfer(Port source, Port destination, Link link, long dataSize, int owner) {
        this.id = next_id++;
        this.src = source;
        this.dest = destination;
        this.link = link;
        this.dataSize = dataSize;
        this.owner = owner;

        // Compute how much we are actually going to transfer
        int mtu = link.getMTU();
        int mss = mtu - PACKET_OVERHEAD;
        long packets = (long) Math.ceil((1.0 * dataSize) / mss);
        long overhead = packets * PACKET_OVERHEAD;
        this.transferSize = dataSize + overhead;

        // Initially we have all the data to transfer remaining and the
        // bandwidth assigned to the transfer is zero
        this.bytesRemaining = transferSize;
        this.currentBandwidth = 0;
        this.lastUpdate = CloudSim.clock();

        this.startTime = 0;
        this.finishTime = 0;
    }

    public long getDataSize() {
        return dataSize;
    }

    public long getTransferSize() {
        return transferSize;
    }

    public long getBytesRemaining() {
        if (bytesRemaining < 0)
            return 0;
        return bytesRemaining;
    }

    public Port getSourcePort() {
        return src;
    }

    public Port getDestinationPort() {
        return dest;
    }

    public Link getLink() {
        return link;
    }

    public int getOwner() {
        return owner;
    }

    public double getCurrentBandwidth() {
        return currentBandwidth;
    }

    /** Set the new bandwidth for the transfer */
    public void updateBandwidth(double newBandwidth) {
        // Set the current bandwidth
        this.currentBandwidth = newBandwidth;

        // Also record the time that the bandwidth was updated
        this.lastUpdate = CloudSim.clock();
    }

    /** Get the RTT for this transfer in ms */
    public double getRTT() {
        return link.getRTT();
    }

    public double getStartTime() {
        return this.startTime;
    }

    public double getFinishTime() {
        return this.finishTime;
    }

    /** Start the transfer */
    protected void start() {
        this.startTime = CloudSim.clock();
    }

    /** Finish the transfer */
    protected void finish() {
        this.finishTime = CloudSim.clock();
    }

    /** Get the total time required for the transfer */
    public double getTransferTime() {
        return this.finishTime - this.startTime;
    }

    /**
     * The transfer is complete when there are no bytes remaining
     * Note that it may be complete, but not finished, if all the bytes have
     * been sent, but we haven't seen the last ACK.
     */
    public boolean isComplete() {
        if (bytesRemaining <= 0) {
            return true;
        }
        return false;
    }

    /** Update the progress of the transfer */
    public void updateProgress() {
        // No need to update if no bandwidth is used
        if (currentBandwidth <= 0) {
            return;
        }

        // Compute how much time has passed since the last time the
        // bandwidth was updated
        double now = CloudSim.clock();
        double elapsed = now - this.lastUpdate;

        // If any time has elapsed, then compute how many bytes remain
        if (elapsed > 0) {

            long bitsTransferred = (long) Math.floor(currentBandwidth * MBPS_TO_BPS * elapsed);
            long bytesTransferred = (long) Math.ceil(bitsTransferred / 8.0);

            // Always make sure at least 1 byte is transferred so
            // we will be sure to make progress
            if (bytesTransferred == 0) {
                bytesTransferred = 1;
            }

            this.bytesRemaining -= bytesTransferred;

            // Sanity check in case we simulate too long
            if (this.bytesRemaining < 0) {
                throw new RuntimeException("Simulated transfer too long. " + "Extra bytes transferred: "
                        + Math.abs(this.bytesRemaining));
            }
        }
    }

    /**
     * Estimate the amount of time remaining for the transfer given the
     * number of bytes remaining to transfer and the current bandwidth
     * @return
     */
    public double estimateTimeRemaining() {
        // Sanity check
        if (CloudSim.clock() != this.lastUpdate) {
            throw new RuntimeException("Tried to estimate time remaining "
                    + "when the bandwidth wasn't updated recently");
        }

        double timeRemaining = bytesRemaining * 8.0 / (this.currentBandwidth * MBPS_TO_BPS);

        return timeRemaining;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (id ^ (id >>> 32));
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
        Transfer other = (Transfer) obj;
        if (id != other.id)
            return false;
        return true;
    }
}
