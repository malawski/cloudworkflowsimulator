package cws.core.jobs;

import cws.core.cloudsim.CloudSimWrapper;
import cws.core.dag.DAGJob;
import cws.core.dag.Task;

/**
 * A job factory that simply creates Jobs.
 * 
 * @author Gideon Juve <juve@usc.edu>
 */
public class SimpleJobFactory implements JobFactory {
    public SimpleJobFactory() {
    }

    @Override
    public Job createJob(DAGJob dagJob, Task task, int owner, CloudSimWrapper cloudsim) {
        Job j = new Job(cloudsim);
        j.setDAGJob(dagJob);
        j.setTask(task);
        j.setOwner(owner);
        return j;
    }
}
