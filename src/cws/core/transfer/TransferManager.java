package cws.core.transfer;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.SimEntity;
import org.cloudbus.cloudsim.core.SimEvent;

import cws.core.WorkflowEvent;

/**
 * This entity simulates data transfers between potentially shared network 
 * ports over potentially shared network links.
 * 
 * Each transfer has a source port, a destination port, and a link.
 * 
 * Ports have fixed bandwidth that is shared between all the transfers that
 * are occurring simultaneously on the port.
 * 
 * Links have fixed bandwidth that is shared by all transfers making use of the
 * link. In addition, links have latency and a fixed MTU.
 * 
 * Transfers follow a simple model where the time taken to complete a transfer
 * depends on the bandwidth assigned to the transfer, the size of the transfer,
 * the amount of transfer overhead (the excess data transferred because of 
 * packet headers), and the round trip time of the link. The formula is,
 * roughly:
 *  
 *     transferTime = ((totalSize + overhead)/bandwidth) + (2 * RTT)
 * 
 * The 2*RTT comes from one RTT for the initial handshake, and one for the 
 * final ACKnowledgement.
 * 
 * Because transfers share ports and links, and transfers may start and stop at 
 * different times, the bandwidth assigned to each transfer may change. Each
 * time a new transfer is started, or an existing transfer completes, we 
 * recompute the bandwidth assigned to every transfer in the simulation. The
 * algorithm used to compute the bandwidth ensures max-min fairness between
 * streams sharing constrained ports and links.
 * 
 * @author Gideon Juve <juve@usc.edu>
 */
public class TransferManager extends SimEntity implements WorkflowEvent {
    /** Conversion constant for milliseconds to seconds */
    public static final double MSEC_TO_SEC = 1.0 / 1000.0;
    
    /** Smallest unit of bandwidth in Mbps */
    public static final double DELTA_BW = 1.0;
    
    /** All the incomplete transfers */
    private HashSet<Transfer> activeTransfers;
    
    public TransferManager() {
        super("TransferManager");
        CloudSim.addEntity(this);
        activeTransfers = new HashSet<Transfer>();
    }
    
    @Override
    public void startEntity() {
        /* Do nothing */
    }
    
    @Override
    public void processEvent(SimEvent ev) {
        switch(ev.getTag()) {
            case NEW_TRANSFER:
                newTransfer((Transfer)ev.getData());
                break;
            case HANDSHAKE_COMPLETE:
                handshakeComplete((Transfer)ev.getData());
                break;
            case UPDATE_TRANSFER_PROGRESS:
                updateProgress();
                break;
            case FINAL_ACK_RECEIVED:
                finalAckReceived((Transfer)ev.getData());
                break;
            default:
                throw new RuntimeException("Unknown event: "+ev);
        }
    }
    
    @Override
    public void shutdownEntity() {
        /* Do nothing */
    }
    
    /** Called when a new transfer is initiated */
    private void newTransfer(Transfer t) {
        // Sanity check
        if (activeTransfers.contains(t)) {
            throw new RuntimeException("Duplicate transfer: "+t);
        }
        
        // Start transfer
        t.start();
        
        // It takes 1 RTT to complete the initial handshake in TCP 
        double rttSec = t.getRTT() * MSEC_TO_SEC;
        send(this.getId(), rttSec, HANDSHAKE_COMPLETE, t);
    }
    
    /** Called when the initial handshake for a transfer is complete */
    private void handshakeComplete(Transfer t) {
        
        // Set the initial bandwidth to 0
        t.updateBandwidth(0.0);
        
        // Add the transfer to the active transfers list
        this.activeTransfers.add(t);
        
        // Update the progress of all transfers
        updateProgress();
    }
    
    /** Update progress of active transfers */
    private void updateProgress() {
        LinkedList<Transfer> completedTransfers = new LinkedList<Transfer>();
        
        // Update the progress of all transfers
        for (Transfer t: activeTransfers) {
            t.updateProgress();
            
            // If transfer is complete, schedule it to be removed
            if (t.isComplete()) {
                completedTransfers.add(t);
            }
        }
        
        // Remove any completed transfers
        for (Transfer t: completedTransfers) {
            activeTransfers.remove(t);
                
            // It takes 1 RTT to get the final ACK
            double rttSec = t.getRTT() * MSEC_TO_SEC;
            send(this.getId(), rttSec, FINAL_ACK_RECEIVED, t);
        }
        
        // If there are still some transfers remaining
        if (activeTransfers.size() > 0) {
        
            // Recompute bandwidth for remaining transfers
            Map<Transfer, Double> allocations = 
                    TransferManager.allocateBandwidth(activeTransfers);
            
            // Update bandwidth
            for (Transfer t: activeTransfers) {
                t.updateBandwidth(allocations.get(t));
            }
            
            // Compute the next completion time and send an update
            double nextUpdate = Double.MAX_VALUE;
            for (Transfer t: activeTransfers) {
                nextUpdate = Math.min(nextUpdate, t.estimateTimeRemaining());
            }
            send(this.getId(), nextUpdate, UPDATE_TRANSFER_PROGRESS);
        }
    }
    
    /** 
     * Called when we need to compute the bandwidth assigned to each
     * transfer.
     */
    public static Map<Transfer, Double> allocateBandwidth(Set<Transfer> transfers) {
        // TODO Make this use arrays instead of maps if it is too slow
        
        HashMap<Transfer, Double> allocations = new HashMap<Transfer, Double>();
        HashMap<Port, Double> ports = new HashMap<Port, Double>();
        HashMap<Link, Double> links = new HashMap<Link, Double>();
        
        for (Transfer t: transfers) {
            Port src = t.getSourcePort();
            Port dest = t.getDestinationPort();
            Link link = t.getLink();
            
            // Allocated bandwidth is initially 0
            allocations.put(t, 0.0);
            
            // All of the ports and links have an initial capacity
            ports.put(src, src.getBandwidth());
            ports.put(dest, dest.getBandwidth());
            links.put(link, link.getBandwidth());
        }
        
        // We keep iterating over the transfers until there is no more
        // available bandwidth to allocate
        boolean change;
        do {
            change = false;
            
            for (Transfer t : transfers) {
                Port src = t.getSourcePort();
                Port dest = t.getDestinationPort();
                Link link = t.getLink();
                
                // Allocated bandwidth
                double tBW = allocations.get(t);
                
                // Available bandwidth
                double srcBW = ports.get(src);
                double destBW = ports.get(dest);
                double linkBW = links.get(link);
                
                // If the src, dest, and link have enough available bandwidth
                if (srcBW >= DELTA_BW &&
                    destBW >= DELTA_BW &&
                    linkBW >= DELTA_BW) {
                    
                    // Decrement available bandwidth
                    ports.put(src, srcBW - DELTA_BW);
                    ports.put(dest, destBW - DELTA_BW);
                    links.put(link, linkBW - DELTA_BW);
                    
                    // Increment allocated bandwidth
                    allocations.put(t, tBW + DELTA_BW);
                    
                    change = true;
                }
            }
        } while (change);
        
        return allocations;
    }
    
    /** Called when the final ACK for a transfer is received */
    private void finalAckReceived(Transfer t) {
        System.out.println("Transfer Complete "+ t);
        
        // Finish the transfer
        t.finish();
        
        // Inform the owner that their transfer is complete
        sendNow(t.getOwner(), TRANSFER_COMPLETE, t); 
    }
}
