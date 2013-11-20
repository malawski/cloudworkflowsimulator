package cws.core;

import cws.core.engine.Environment;

/**
 * An interface for job schedulers used by the WorkflowEngine.
 * 
 * @author Gideon Juve <juve@usc.edu>
 */
public interface Scheduler {
    public void scheduleJobs(WorkflowEngine engine);

    public void setEnvironment(Environment environment);
}
