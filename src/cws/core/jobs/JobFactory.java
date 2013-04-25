package cws.core.jobs;

import cws.core.cloudsim.CloudSimWrapper;
import cws.core.dag.DAGJob;
import cws.core.dag.Task;

public interface JobFactory {
    public Job createJob(DAGJob dagJob, Task task, int owner, CloudSimWrapper cloudsim);
}
