package cws.core;

public interface JobListener {
    /** Job has been released (i.e. its parents are done) */
    public void jobReleased(Job job);

    /** Job has been matched with a resource */
    public void jobSubmitted(Job job);

    /** Job began executing */
    public void jobStarted(Job job);

    /** Job finished executing */
    public void jobFinished(Job job);
}
