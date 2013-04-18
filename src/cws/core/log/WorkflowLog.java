package cws.core.log;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.DecimalFormat;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.cloudbus.cloudsim.Log;

import cws.core.DAGJob;
import cws.core.DAGJobListener;
import cws.core.Job;
import cws.core.JobListener;
import cws.core.VM;
import cws.core.VMListener;

public class WorkflowLog implements JobListener, VMListener, DAGJobListener {

    Set<Job> jobs = new HashSet<Job>();
    Set<VM> vms = new HashSet<VM>();
    Set<DAGJob> djs = new HashSet<DAGJob>();

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

    public void printJobs(String fileName) {

        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw, true);

        String indent = "    ";
        pw.println();
        pw.println("========== OUTPUT ==========");
        pw.println("Job ID" + indent + "STATUS" + indent + "Priority" + indent + "VM ID" + indent + "Time" + indent
                + "Start Time" + indent + "Finish Time");

        DecimalFormat dft = new DecimalFormat("###.##");
        for (Job job : jobs) {
            pw.print(indent + job.getID() + indent + indent);

            if (job.getState() == Job.State.TERMINATED && job.getResult() == Job.Result.SUCCESS) {
                pw.print("SUCCESS");
                
                //FIXME: temporary hack - detecting data transfer job
                if(job.getDAGJob() == null) continue;

                pw.println(indent + indent + job.getDAGJob().getPriority() + indent + indent + indent
                        + job.getVM().getId() + indent + indent + dft.format(job.getDuration()) + indent + indent
                        + dft.format(job.getStartTime()) + indent + indent + dft.format(job.getFinishTime()));
            } else {
                pw.print("FAILED");
                

                //FIXME: temporary hack - detecting data transfer job
                if(job.getDAGJob() == null) continue;
                
                pw.println(indent + indent + job.getDAGJob().getPriority() + indent + indent + indent
                        + job.getVM().getId() + indent + indent + dft.format(job.getDuration()) + indent + indent
                        + dft.format(job.getStartTime()) + indent + indent + dft.format(job.getFinishTime()));

            }
        }
        // Log.print(sw.toString());
        stringToFile(sw.toString(), fileName + ".txt");

    }

    public void printVmList(String name) {

        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw, true);

        String indent = "    ";
        pw.println();
        pw.println("========== VMs ==========");
        pw.println("VM ID" + indent + "Creation Time" + indent + "Destroy Time");

        DecimalFormat dft = new DecimalFormat("###.##");

        for (VM vm : vms) {

            pw.print(indent + vm.getId() + indent + indent);

            pw.println(indent + indent + dft.format(vm.getLaunchTime()) + indent + indent
                    + dft.format(vm.getTerminateTime()));
        }

        // Log.print(sw.toString());
        stringToFile(sw.toString(), name + "-vms.txt");

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
        Log.print(sw.toString());

    }

    public static void stringToFile(String s, String fileName) {
        BufferedWriter out = null;
        try {
            out = new BufferedWriter(new FileWriter(fileName));
            out.write(s);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            IOUtils.closeQuietly(out);
        }
    }
}
