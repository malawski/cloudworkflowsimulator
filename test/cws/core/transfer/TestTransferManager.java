package cws.core.transfer;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.SimEntity;
import org.cloudbus.cloudsim.core.SimEvent;
import org.junit.Test;

import cws.core.WorkflowEvent;

public class TestTransferManager {
    public static final long KB = 1024;
    public static final long MB = 1024 * KB;
    public static final long GB = 1024 * MB;

    private class TransferDriver extends SimEntity implements WorkflowEvent {
        private TransferManager tm;
        private List<Transfer> transfers;

        public TransferDriver() {
            super("TransferDriver");
            this.tm = new TransferManager();
            CloudSim.addEntity(this);
        }

        public void setTransfers(List<Transfer> transfers) {
            this.transfers = transfers;
        }

        @Override
        public void startEntity() {
            Random rng = new Random(0);
            // Submit all the transfers
            for (Transfer t : transfers) {
                send(tm.getId(), rng.nextDouble(), NEW_TRANSFER, t);
            }
        }

        @Override
        public void processEvent(SimEvent ev) {
        }

        @Override
        public void shutdownEntity() {
        }
    }

    @Test
    public void testSimpleTransfer24() {
        CloudSim.init(1, null, false);

        Random rng = new Random(7);

        TransferDriver td = new TransferDriver();

        Port a = new Port(1000);
        Port[] b = new Port[4];
        for (int i = 0; i < 4; i++) {
            b[i] = new Port(1000);
        }
        Link l = new Link(1000, 1000.0);
        List<Transfer> transfers = new ArrayList<Transfer>();

        for (int i = 0; i < 24; i++) {
            Transfer t1 = new Transfer(a, b[i % 4], l, (int) (100 * rng.nextDouble()) * MB, td.getId());
            transfers.add(t1);
        }

        td.setTransfers(transfers);

        CloudSim.startSimulation();

    }

}
