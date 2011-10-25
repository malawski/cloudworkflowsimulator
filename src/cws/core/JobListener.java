package cws.core;

public interface JobListener {
    public void jobQueued(Job job);
    public void jobStarted(Job job);
    public void jobFinished(Job job);
}
