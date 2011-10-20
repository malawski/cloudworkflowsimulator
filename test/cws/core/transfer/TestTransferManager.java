package cws.core.transfer;

import java.util.HashSet;
import java.util.Map;

import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.SimEntity;
import org.cloudbus.cloudsim.core.SimEvent;
import org.junit.Test;
import org.junit.Before;
import static org.junit.Assert.*;

import cws.core.WorkflowEvent;

public class TestTransferManager  {
    public static final long KB = 1024;
    public static final long MB = 1024 * KB;
    public static final long GB = 1024 * MB;
    
    private double DELTA;
    
    @Before
    public void setup() {
        DELTA = TransferManager.DELTA_BW + TransferManager.DELTA_BW/10.0;
    }
    
    @Test
    public void testAllocateDestPortLimited() {
        Port a = new Port(100);
        Port b = new Port(100);
        Port c = new Port(100);
        Link l = new Link(1000, 0);
        
        Transfer t1 = new Transfer(a, c, l, 0L, 0);
        Transfer t2 = new Transfer(b, c, l, 0L, 0);
        
        HashSet<Transfer> transfers = new HashSet<Transfer>();
        transfers.add(t1);
        transfers.add(t2);
        
        Map<Transfer, Double> allocations = 
                TransferManager.allocateBandwidth(transfers);
        
        for (Transfer t: transfers) {
            assertEquals(50.0, allocations.get(t), DELTA);
        }
    }
    
    @Test
    public void testAllocateSourcePortLimited() {
        Port a = new Port(100);
        Port b = new Port(100);
        Port c = new Port(1000);
        Link l = new Link(1000, 0);
        
        Transfer t1 = new Transfer(a, c, l, 0L, 0);
        Transfer t2 = new Transfer(b, c, l, 0L, 0);
        
        HashSet<Transfer> transfers = new HashSet<Transfer>();
        transfers.add(t1);
        transfers.add(t2);
        
        Map<Transfer, Double> allocations = 
                TransferManager.allocateBandwidth(transfers);
        
        for (Transfer t: transfers) {
            assertEquals(100.0, allocations.get(t), DELTA);
        }
    }
    
    @Test
    public void testAllocateLinkLimited() {
        Port a = new Port(100);
        Port b = new Port(100);
        Port c = new Port(100);
        Link l = new Link(50, 0);
        
        Transfer t1 = new Transfer(a, c, l, 0L, 0);
        Transfer t2 = new Transfer(b, c, l, 0L, 0);
        
        HashSet<Transfer> transfers = new HashSet<Transfer>();
        transfers.add(t1);
        transfers.add(t2);
        
        Map<Transfer, Double> allocations = 
                TransferManager.allocateBandwidth(transfers);
        
        for (Transfer t: transfers) {
            assertEquals(25.0, allocations.get(t), DELTA);
        }
    }
    
    @Test
    public void testAllocateThreeTransfers() {
        Port a = new Port(100);
        Port b = new Port(100);
        Port c = new Port(100);
        Link l = new Link(1000, 0);
        
        Transfer t1 = new Transfer(a, c, l, 0L, 0);
        Transfer t2 = new Transfer(b, c, l, 0L, 0);
        Transfer t3 = new Transfer(a, c, l, 0L, 0);
        
        HashSet<Transfer> transfers = new HashSet<Transfer>();
        transfers.add(t1);
        transfers.add(t2);
        transfers.add(t3);
        
        Map<Transfer, Double> allocations = 
                TransferManager.allocateBandwidth(transfers);
        
        for (Transfer t: transfers) {
            assertEquals(100/3.0, allocations.get(t), DELTA);
        }
    }
    
    @Test
    public void testAllocateFairness() {
        Port a = new Port(100);
        Port b = new Port(100);
        Port c = new Port(160);
        Port d = new Port(30);
        Link l = new Link(1000, 0);
        
        Transfer t1 = new Transfer(a, c, l, 0L, 0);
        Transfer t2 = new Transfer(b, c, l, 0L, 0);
        Transfer t3 = new Transfer(b, d, l, 0L, 0);
        
        HashSet<Transfer> transfers = new HashSet<Transfer>();
        transfers.add(t1);
        transfers.add(t2);
        transfers.add(t3);
        
        Map<Transfer, Double> allocations = 
                TransferManager.allocateBandwidth(transfers);
        
        assertEquals(90.0, allocations.get(t1), DELTA);
        assertEquals(70.0, allocations.get(t2), DELTA);
        assertEquals(30.0, allocations.get(t3), DELTA);
    }
    
    @Test
    public void testAllocateOddBandwidth() {
        Port a = new Port(100);
        Port b = new Port(100);
        Port c = new Port(5);
        Link l = new Link(1000, 0);
        
        Transfer t1 = new Transfer(a, c, l, 0L, 0);
        Transfer t2 = new Transfer(b, c, l, 0L, 0);
        
        HashSet<Transfer> transfers = new HashSet<Transfer>();
        transfers.add(t1);
        transfers.add(t2);
        
        Map<Transfer, Double> allocations = 
                TransferManager.allocateBandwidth(transfers);
        
        assertEquals(2.5, allocations.get(t1), DELTA);
        assertEquals(2.5, allocations.get(t2), DELTA);
    }
    
    @Test
    public void testAllocateDuplicateTransfers() {
        Port a = new Port(100);
        Port b = new Port(100);
        Link l = new Link(1000, 0);
        
        Transfer t1 = new Transfer(a, b, l, 0L, 0);
        Transfer t2 = new Transfer(a, b, l, 0L, 0);
        
        HashSet<Transfer> transfers = new HashSet<Transfer>();
        transfers.add(t1);
        transfers.add(t2);
        
        Map<Transfer, Double> allocations = 
                TransferManager.allocateBandwidth(transfers);
        
        assertEquals(50, allocations.get(t1), DELTA);
        assertEquals(50, allocations.get(t2), DELTA);
    }
    
    @Test
    public void testAllocateFairShareWithDuplicates() {
        Port a = new Port(100);
        Port b = new Port(100);
        Port c = new Port(160);
        Port d = new Port(30);
        Link l = new Link(1000, 0);
        
        Transfer t1 = new Transfer(a, c, l, 0L, 0);
        Transfer t2 = new Transfer(b, c, l, 0L, 0);
        Transfer t3 = new Transfer(b, d, l, 0L, 0);
        Transfer t4 = new Transfer(b, d, l, 0L, 0);
        
        HashSet<Transfer> transfers = new HashSet<Transfer>();
        transfers.add(t1);
        transfers.add(t2);
        transfers.add(t3);
        transfers.add(t4);
        
        Map<Transfer, Double> allocations = 
                TransferManager.allocateBandwidth(transfers);
        
        assertEquals(90.0, allocations.get(t1), DELTA);
        assertEquals(70.0, allocations.get(t2), DELTA);
        assertEquals(15.0, allocations.get(t3), DELTA);
        assertEquals(15.0, allocations.get(t4), DELTA);
    }
    
    @Test
    public void testAllocateScaling() {
        HashSet<Transfer> transfers = new HashSet<Transfer>();
        
        for (int i=0; i<20000; i++) {
            Port a = new Port(100);
            Port b = new Port(100);
            Link l = new Link(100, 0);
            Transfer t = new Transfer(a, b, l, 0, 0);
            transfers.add(t);
        }
        
        double start = System.currentTimeMillis();
        Map<Transfer, Double> allocations = 
                TransferManager.allocateBandwidth(transfers);
        double finish = System.currentTimeMillis();
        double elapsed = (finish-start)/1000;
        
        for (Transfer t : transfers) {
            assertEquals(100.0, allocations.get(t), DELTA);
        }
        
        assertTrue("BW allocation too slow", elapsed <= 1.0);
    }
    
    private class TransferDriver extends SimEntity implements WorkflowEvent {
        private TransferManager tm;
        private HashSet<Transfer> transfers;
        
        public TransferDriver() {
            super("TransferDriver");
            this.tm = new TransferManager();
            CloudSim.addEntity(this);
        }
        
        public void setTransfers(HashSet<Transfer> transfers) {
            this.transfers = transfers;
        }
        
        @Override
        public void startEntity() {
            // Submit all the transfers
            for (Transfer t: transfers) {
                send(tm.getId(), 0.0, NEW_TRANSFER, t);
            }
        }
        
        @Override
        public void processEvent(SimEvent ev) { }
        
        @Override
        public void shutdownEntity() { }
    }
    
    public double estimateTransferTime(long size, double bandwidth, double rtt) {
        // SIZE/BW + 2*RTT
        return ((size*8.0) / (bandwidth*1000000)) + 
                (2*(rtt*TransferManager.MSEC_TO_SEC));
    }
    
    @Test
    public void testSimpleTransfer() {
        CloudSim.init(1, null, false);
        
        TransferDriver td = new TransferDriver();
        
        Port a = new Port(1000);
        Port b = new Port(1000);
        Link l = new Link(1000, 1.0);
        
        Transfer t1 = new Transfer(a, b, l, 100*MB, td.getId());
        
        HashSet<Transfer> transfers = new HashSet<Transfer>();
        transfers.add(t1);
        
        td.setTransfers(transfers);
        
        CloudSim.startSimulation();
        
        double time1 = estimateTransferTime(
                t1.getTransferSize(), 1000.0, t1.getRTT());
        
        assertEquals(time1, t1.getTransferTime(), 0.0001);
    }
    
    @Test
    public void testSimpleTransfer2() {
        CloudSim.init(1, null, false);
        
        TransferDriver td = new TransferDriver();
        
        Port a = new Port(1000);
        Port b = new Port(1000);
        Link l = new Link(100, 150.0);
        
        Transfer t1 = new Transfer(a, b, l, 100*MB, td.getId());
        
        HashSet<Transfer> transfers = new HashSet<Transfer>();
        transfers.add(t1);
        
        td.setTransfers(transfers);
        
        CloudSim.startSimulation();
        
        
        double time1 = estimateTransferTime(
                t1.getTransferSize(), 100.0, t1.getRTT());
        
        assertEquals(time1, t1.getTransferTime(), 0.0001);
    }
    
    @Test
    public void testSimpleTransfer3() {
        CloudSim.init(1, null, false);
        
        TransferDriver td = new TransferDriver();
        
        Port a = new Port(1000);
        Port b = new Port(1000);
        Port c = new Port(1000);
        Link l = new Link(1000, 150.0);
        
        Transfer t1 = new Transfer(a, b, l, 100*MB, td.getId());
        Transfer t2 = new Transfer(a, c, l, 100*MB, td.getId());
        
        HashSet<Transfer> transfers = new HashSet<Transfer>();
        transfers.add(t1);
        transfers.add(t2);
        
        td.setTransfers(transfers);
        
        CloudSim.startSimulation();
        
        
        double time1 = estimateTransferTime(
                t1.getTransferSize(), 500.0, t1.getRTT());
        assertEquals(time1, t1.getTransferTime(), 0.0001);
        
        double time2 = estimateTransferTime(
                t2.getTransferSize(), 500.0, t2.getRTT());
        assertEquals(time2, t2.getTransferTime(), 0.0001);
    }
}
