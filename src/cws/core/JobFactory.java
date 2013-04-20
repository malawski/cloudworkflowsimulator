package cws.core;

import cws.core.dag.Task;

public interface JobFactory {
    public Job createJob(DAGJob dagJob, Task task, int owner, double releaseTime);
}
