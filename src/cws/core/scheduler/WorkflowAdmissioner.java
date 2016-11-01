package cws.core.scheduler;

import cws.core.VM;
import cws.core.WorkflowEngine;
import cws.core.core.VMType;
import cws.core.jobs.Job;

/**
 * Service which decides whether a DAG should be admissioned.
 */
public interface WorkflowAdmissioner {
    boolean isJobDagAdmitted(Job job, WorkflowEngine engine, VM vm);
    VMType getVmType();
}
