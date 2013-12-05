package cws.core.transfer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;

import cws.core.WorkflowEvent;
import cws.core.cloudsim.CWSSimEntity;
import cws.core.cloudsim.CWSSimEvent;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.exception.UnknownWorkflowEventException;

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
 * transferTime = ((totalSize + overhead)/bandwidth) + (2 * RTT)
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
public class TransferManager extends CWSSimEntity {
    /** Conversion constant for milliseconds to seconds */
    public static final double MSEC_TO_SEC = 1.0 / 1000.0;

    /** All the incomplete transfers */
    private HashSet<Transfer> activeTransfers;

    /** Listeners for transfer events */
    private HashSet<TransferListener> listeners;

    public TransferManager(CloudSimWrapper cloudsim) {
        super("TransferManager", cloudsim);
        activeTransfers = new HashSet<Transfer>();
        listeners = new HashSet<TransferListener>();
    }

    public void addListener(TransferListener tl) {
        listeners.add(tl);
    }

    public void removeListener(TransferListener tl) {
        listeners.remove(tl);
    }

    @Override
    public void startEntity() {
        /* Do nothing */
    }

    @Override
    public void processEvent(CWSSimEvent ev) {
        switch (ev.getTag()) {
        case WorkflowEvent.NEW_TRANSFER:
            newTransfer((Transfer) ev.getData());
            break;
        case WorkflowEvent.HANDSHAKE_COMPLETE:
            handshakeComplete((Transfer) ev.getData());
            break;
        case WorkflowEvent.UPDATE_TRANSFER_PROGRESS:
            updateProgress();
            break;
        case WorkflowEvent.FINAL_ACK_RECEIVED:
            finalAckReceived((Transfer) ev.getData());
            break;
        default:
            throw new UnknownWorkflowEventException("Unknown event: " + ev);
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
            throw new RuntimeException("Duplicate transfer: " + t);
        }

        // Start transfer
        t.start();

        // Notify listeners
        for (TransferListener tl : listeners) {
            tl.transferStarted(t);
        }

        // It takes 1 RTT to complete the initial handshake in TCP
        double rttSec = t.getRTT() * MSEC_TO_SEC;
        getCloudsim().send(getId(), getId(), rttSec, WorkflowEvent.HANDSHAKE_COMPLETE, t);
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
        // Log.printLine(CloudSim.clock() + " Transfer manager: updating progress, active transfers: " +
        // activeTransfers.size());

        LinkedList<Transfer> completedTransfers = new LinkedList<Transfer>();

        // Update the progress of all transfers
        for (Transfer t : activeTransfers) {
            t.updateProgress();

            // If transfer is complete, schedule it to be removed
            if (t.isComplete()) {
                completedTransfers.add(t);
            }
        }

        // Remove any completed transfers
        for (Transfer t : completedTransfers) {
            activeTransfers.remove(t);

            // It takes 1 RTT to get the final ACK
            double rttSec = t.getRTT() * MSEC_TO_SEC;
            getCloudsim().send(getId(), getId(), rttSec, WorkflowEvent.FINAL_ACK_RECEIVED, t);
        }

        // If there are still some transfers remaining
        if (activeTransfers.size() > 0) {

            // Recompute bandwidth for remaining transfers
            Transfer[] transfers = activeTransfers.toArray(new Transfer[0]);
            double[] allocations = TransferManager.allocateBandwidth(transfers);

            // Update bandwidth
            for (int i = 0; i < transfers.length; i++) {

                // Did bandwidth change by more than 1bps?
                boolean changed = Math.abs(transfers[i].getCurrentBandwidth() - allocations[i]) >= 0.000001;

                // Update bandwidth
                transfers[i].updateBandwidth(allocations[i]);

                // If bandwidth changed, notify listeners
                if (changed) {
                    for (TransferListener tl : listeners) {
                        tl.bandwidthChanged(transfers[i]);
                    }
                }
            }

            // Compute the next completion time and send an update
            double nextUpdate = Double.MAX_VALUE;
            for (Transfer t : activeTransfers) {
                nextUpdate = Math.min(nextUpdate, t.estimateTimeRemaining());
            }
            getCloudsim().send(getId(), getId(), nextUpdate, WorkflowEvent.UPDATE_TRANSFER_PROGRESS);
        }
    }

    /** A node is a network element with a bandwidth capacity */
    private static class Node {
        public HashSet<Flow> flows = new HashSet<Flow>();
        public double capacity = 0.0;
    }

    /** A flow is the bandwidth allocated to a transfer */
    private static class Flow {
        public Node[] path = new Node[3];
        public double allocation = 0.0;
    }

    /**
     * Called when we need to compute the bandwidth assigned to each
     * transfer. This uses the progressive filling algorithm.
     */
    public static double[] allocateBandwidth(Transfer[] transfers) {

        Flow[] flows = new Flow[transfers.length];
        ArrayList<Node> nodes = new ArrayList<Node>();

        // These are used to find the set of unique Ports and Links
        HashMap<Port, Node> ports = new HashMap<Port, Node>();
        HashMap<Link, Node> links = new HashMap<Link, Node>();

        // This loop just sets up the data structures
        for (int i = 0; i < transfers.length; i++) {
            Node n;

            // Create a flow for each transfer
            Transfer t = transfers[i];
            Flow f = flows[i] = new Flow();

            // Add the source port
            Port src = t.getSourcePort();
            if (ports.containsKey(src)) {
                n = ports.get(src);
            } else {
                n = new Node();
                nodes.add(n);
                n.capacity = src.getBandwidth();
                ports.put(src, n);
            }
            n.flows.add(f);
            f.path[0] = n;

            // Add the destination port
            Port dest = t.getDestinationPort();
            if (ports.containsKey(dest)) {
                n = ports.get(dest);
            } else {
                n = new Node();
                nodes.add(n);
                n.capacity = dest.getBandwidth();
                ports.put(dest, n);
            }
            n.flows.add(f);
            f.path[1] = n;

            // Add the link
            Link link = t.getLink();
            if (links.containsKey(link)) {
                n = links.get(link);
            } else {
                n = new Node();
                nodes.add(n);
                n.capacity = link.getBandwidth();
                links.put(link, n);
            }
            n.flows.add(f);
            f.path[2] = n;
        }

        // As long as there are nodes remaining that have flows
        int nnodes = nodes.size();
        while (nnodes > 0) {

            // Find the node with the smallest remaining fair share
            Node minNode = null;
            double minShare = Double.MAX_VALUE;
            for (Node n : nodes) {
                double share = n.capacity / n.flows.size();
                if (share <= minShare) {
                    minShare = share;
                    minNode = n;
                }
            }

            // Allocate the min share to each flow that uses the min node
            Flow[] myflows = minNode.flows.toArray(new Flow[0]);
            for (Flow f : myflows) {
                f.allocation += minShare;
                for (Node n : f.path) {
                    n.capacity -= minShare;
                    n.flows.remove(f);
                }
            }

            // Remove all nodes with no remaining flows
            int i = 0;
            while (i < nnodes) {
                Node n = nodes.get(i);
                if (n.flows.size() == 0) {
                    // Swap with the last node
                    nnodes--;
                    nodes.set(i, nodes.get(nnodes));
                } else {
                    i++;
                }
            }
        }

        // Return allocations
        double[] allocations = new double[transfers.length];
        for (int i = 0; i < transfers.length; i++) {
            allocations[i] = flows[i].allocation;
        }
        return allocations;
    }

    /** Called when the final ACK for a transfer is received */
    private void finalAckReceived(Transfer t) {
        System.out.println("Transfer Complete " + t);

        // Finish the transfer
        t.finish();

        // Notify listeners
        for (TransferListener tl : listeners) {
            tl.transferFinished(t);
        }

        // Inform the owner that their transfer is complete
        sendNow(t.getOwner(), WorkflowEvent.TRANSFER_COMPLETE, t);
    }
}
