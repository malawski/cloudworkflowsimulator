package cws.core;

import cws.core.cloudsim.CloudSimWrapper;
import cws.core.dag.Task;

/**
 * A job factory that scales the size of a task by some scaling factor.
 * 
 * @author Gideon Juve <juve@usc.edu>
 */
public class SimpleJobFactory implements JobFactory {
    double scale;

    public SimpleJobFactory() {
        this.scale = 1;
    }

    public SimpleJobFactory(double scale) {
        this.scale = scale;
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
