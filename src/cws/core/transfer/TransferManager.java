package cws.core.transfer;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;

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
    public static final double DELTA_BW = 0.1;
    
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
            Transfer[] transfers = activeTransfers.toArray(new Transfer[0]);
            double[] allocations = 
                    TransferManager.allocateBandwidth(transfers);
            
            // Update bandwidth
            for (int i=0; i<transfers.length; i++) {
                transfers[i].updateBandwidth(allocations[i]);
            }
            
            // Compute the next completion time and send an update
            double nextUpdate = Double.MAX_VALUE;
            for (Transfer t: activeTransfers) {
                nextUpdate = Math.min(nextUpdate, t.estimateTimeRemaining());
            }
            send(this.getId(), nextUpdate, UPDATE_TRANSFER_PROGRESS);
        }
    }
    
    private static class Path {
        public int src;
        public int dest;
        public int link;
    }
    
    /** 
     * Called when we need to compute the bandwidth assigned to each
     * transfer.
     */
    public static double[] allocateBandwidth(Transfer[] transfers) {
        
        HashMap<Port, Integer> port_map = new HashMap<Port, Integer>();
        HashMap<Link, Integer> link_map = new HashMap<Link, Integer>();
        int next_bw = 0;
        
        Path[] paths = new Path[transfers.length];
        double[] allocations = new double[transfers.length];
        
        for (int i=0; i<transfers.length; i++) {
            Transfer t = transfers[i];
            Port src = t.getSourcePort();
            Port dest = t.getDestinationPort();
            Link link = t.getLink();
            
            // Allocated bandwidth is initially 0
            allocations[i] = 0.0;
            
            // Keep track of all the paths
            Path p = new Path();
            paths[i] = p;
            
            if (port_map.containsKey(src)) {
                p.src = port_map.get(src);
            } else {
                p.src = next_bw++;
                port_map.put(src, p.src);
            }
            
            if (port_map.containsKey(dest)) {
                p.dest = port_map.get(dest);
            } else {
                p.dest = next_bw++;
                port_map.put(dest, p.dest);
            }
            
            if (link_map.containsKey(link)) {
                p.link = link_map.get(link);
            } else {
                p.link = next_bw++;
                link_map.put(link, p.link);
            }
        }
        
        double[] bw = new double[next_bw];
        
        for (Port p : port_map.keySet()) {
            int index = port_map.get(p);
            bw[index] = p.getBandwidth();
        }
        
        for (Link l : link_map.keySet()) {
            int index = link_map.get(l);
            bw[index] = l.getBandwidth();
        }
        
        link_map = null;
        port_map = null;
        
        
        // We keep iterating over the transfers until there is no more
        // available bandwidth to allocate
        boolean change;
        do {
            change = false;
        
            for (int i=0; i<paths.length; i++) {
                Path p = paths[i];
            
                // If the src, dest, and link have available bandwidth
                if (bw[p.src] >= DELTA_BW &&
                    bw[p.dest] >= DELTA_BW &&
                    bw[p.link] >= DELTA_BW) {
                    
                    // Decrement available bandwidth
                    bw[p.src] -= DELTA_BW;
                    bw[p.dest] -= DELTA_BW;
                    bw[p.link] -= DELTA_BW;
                    
                    // Increment allocated bandwidth
                    allocations[i] += DELTA_BW;
                    
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
