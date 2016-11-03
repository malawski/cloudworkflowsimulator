package cws.core.scheduler;

import cws.core.VM;
import cws.core.WorkflowEngine;
import cws.core.core.VMType;
import cws.core.jobs.Job;

/**
 * Workflow admissioner that always admissions workflows.
 */
public final class VoidWorkflowAdmissioner implements WorkflowAdmissioner {
    private final VMType selectedVmType;

    public VoidWorkflowAdmissioner(VMType selectedVmType) {
        this.selectedVmType = selectedVmType;
    }

    @Override
    public boolean isJobDagAdmitted(Job job, WorkflowEngine engine, VM vm) {
        return true;
    }

    public VMType getSelectedVmType() {
        return this.selectedVmType;
    }
}
