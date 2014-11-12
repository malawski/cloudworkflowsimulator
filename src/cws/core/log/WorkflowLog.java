package cws.core.log;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.DecimalFormat;
import java.util.LinkedHashSet;
import java.util.Set;

import cws.core.VM;
import cws.core.VMListener;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.dag.DAGJob;
import cws.core.dag.DAGJobListener;
import cws.core.jobs.Job;
import cws.core.jobs.JobListener;

/**
 * Gathers and logs information about jobs, VMs and DAGs running/completed during a simulation.
 */
public class WorkflowLog implements JobListener, VMListener, DAGJobListener {
    private Set<Job> jobs = new LinkedHashSet<Job>();
    private Set<VM> vms = new LinkedHashSet<VM>();
    private Set<DAGJob> djs = new LinkedHashSet<DAGJob>();
    private CloudSimWrapper cloudsim;

    public WorkflowLog(CloudSimWrapper cloudsim) {
        this.cloudsim = cloudsim;
    }

    public void printJobs() {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw, true);

        String indent = "    ";
        pw.println();
        pw.println("========== OUTPUT ==========");
        pw.println("  Job ID " + indent + "STATUS  " + indent + "Priority  " + indent + "VM ID  " + indent + "Time  "
                + indent + "Start Time  " + indent + "Finish Time");

        DecimalFormat dft = new DecimalFormat("###.##");
        for (Job job : jobs) {
            pw.print(indent + job.getID() + indent + indent);

            if (job.getState() == Job.State.TERMINATED && job.getResult() == Job.Result.SUCCESS) {
                pw.print("SUCCESS");

                String priority = "";
                // FIXME: temporary hack - detecting data transfer job
                if (job.getDAGJob() != null) {
                    priority = String.format("%d", job.getDAGJob().getPriority());
                }

                pw.println(indent + indent + priority + indent + indent + indent + job.getVM().getId() + indent
                        + indent + dft.format(job.getDuration()) + indent + indent + dft.format(job.getStartTime())
                        + indent + indent + dft.format(job.getFinishTime()));
            } else {
                pw.print("FAILED");

                String priority = "";
                // FIXME: temporary hack - detecting data transfer job
                if (job.getDAGJob() != null) {
                    priority = String.format("%d", job.getDAGJob().getPriority());
                }

                pw.println(indent + indent + priority + indent + indent + indent + job.getVM().getId() + indent
                        + indent + dft.format(job.getDuration()) + indent + indent + dft.format(job.getStartTime())
                        + indent + indent + dft.format(job.getFinishTime()));

            }
        }
        cloudsim.log(sw.toString());
    }

    public void printVmList() {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw, true);

        String indent = "    ";
        pw.println();
        pw.println("========== VMs ==========");
        pw.println("VM ID" + indent + "Creation Time" + indent + "Destroy Time");

        DecimalFormat dft = new DecimalFormat("###.##");

        for (VM vm : vms) {
            if (!vm.isTerminated()) {
                throw new RuntimeException("VM is not terminated: " + vm.getId());
            }
            pw.print(indent + vm.getId() + indent + indent);

            pw.println(indent + indent + dft.format(vm.getLaunchTime()) + indent + indent
                    + dft.format(vm.getTerminateTime()));
        }

        cloudsim.log(sw.toString());
    }

    public void printDAGJobs() {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw, true);

        String indent = "    ";
        pw.println();
        pw.println("========== DAGJobs ==========");
        pw.println("Priority" + indent + "Finished");

        int finished = 0;
        for (DAGJob dj : djs) {
            pw.print(indent + dj.getPriority() + indent + indent);
            pw.print(indent + dj.isFinished() + indent + indent);
            pw.println();
            if (dj.isFinished())
                finished++;
        }
        pw.println("Completed DAGs: " + finished);
        cloudsim.log(sw.toString());
    }

    @Override
    public void jobReleased(Job job) {
    }

    @Override
    public void jobSubmitted(Job job) {
    }

    @Override
    public void jobStarted(Job job) {
    }

    @Override
    public void jobFinished(Job job) {
        jobs.add(job);
    }

    @Override
    public void vmLaunched(VM vm) {
        vms.add(vm);
    }

    @Override
    public void vmTerminated(VM vm) {
    }

    @Override
    public void dagStarted(DAGJob dagJob) {
    }

    @Override
    public void dagFinished(DAGJob dagJob) {
        djs.add(dagJob);
    }
}
