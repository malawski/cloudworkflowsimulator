package cws.core.emulator;

import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.core.CloudSim;

import cws.core.Job;
import cws.core.VM;
import cws.core.WorkflowEngine;
import cws.core.WorkflowEvent;

public class CloudEmulator {

	public void submitJob(WorkflowEngine engine, VM vm, Job job) {
		Log.printLine(CloudSim.clock() + " Submitting job " + job.getTask().getId() + " to VM " + job.getVM().getId());
		CloudSim.send(engine.getId(), vm.getId(), 0.0, WorkflowEvent.JOB_SUBMIT, job);
	}

}
