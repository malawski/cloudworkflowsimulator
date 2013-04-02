package cws.core.transfer;

public interface TransferListener {
    public void transferStarted(Transfer t);

    public void bandwidthChanged(Transfer t);

    public void transferFinished(Transfer t);
}
