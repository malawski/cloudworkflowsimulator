package cws.core.transfer;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.Before;
import org.junit.Test;

import cws.core.WorkflowEvent;
import cws.core.cloudsim.CWSSimEntity;
import cws.core.cloudsim.CWSSimEvent;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.transfer.Link;
import cws.core.transfer.Port;
import cws.core.transfer.Transfer;
import cws.core.transfer.TransferManager;

public class RandomTransferManagerTest {
    public static final long KB = 1024;
    public static final long MB = 1024 * KB;
    public static final long GB = 1024 * MB;

    private class TransferDriver extends CWSSimEntity {
        private TransferManager tm;
        private List<Transfer> transfers;

        public TransferDriver(CloudSimWrapper cloudsim) {
            super("TransferDriver", cloudsim);
            this.tm = new TransferManager(cloudsim);
        }

        public void setTransfers(List<Transfer> transfers) {
            this.transfers = transfers;
        }

        @Override
        public void startEntity() {
            Random rng = new Random(0);
            // Submit all the transfers
            for (Transfer t : transfers) {
                getCloudsim().send(getId(), tm.getId(), rng.nextDouble(), WorkflowEvent.NEW_TRANSFER, t);
            }
        }

        @Override
        public void processEvent(CWSSimEvent ev) {
        }

        @Override
        public void shutdownEntity() {
        }
    }

    private CloudSimWrapper cloudsim;

    @Before
    public void setUp() {
        // TODO(_mequrel_): change to IoC in the future or to mock
        cloudsim = new CloudSimWrapper();
        cloudsim.init();
    }

    @Test
    public void testSimpleTransfer24() {
        Random rng = new Random(7);

        TransferDriver td = new TransferDriver(cloudsim);

        Port a = new Port(1000);
        Port[] b = new Port[4];
        for (int i = 0; i < 4; i++) {
            b[i] = new Port(1000);
        }
        Link l = new Link(1000, 1000.0);
        List<Transfer> transfers = new ArrayList<Transfer>();

        for (int i = 0; i < 24; i++) {
            Transfer t1 = new Transfer(a, b[i % 4], l, (int) (100 * rng.nextDouble()) * MB, td.getId(), cloudsim);
            transfers.add(t1);
        }

        td.setTransfers(transfers);

        cloudsim.startSimulation();
    }
}
