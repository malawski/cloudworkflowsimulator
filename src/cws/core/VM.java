package cws.core;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;

import cws.core.cloudsim.CWSSimEntity;
import cws.core.cloudsim.CWSSimEvent;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.exception.UnknownWorkflowEventException;
import cws.core.jobs.IdentityRuntimeDistribution;
import cws.core.jobs.Job;
import cws.core.jobs.RuntimeDistribution;
import cws.core.provisioner.VMFactory;
import cws.core.storage.cache.VMCacheManager;

/**
 * A VM is a virtual machine that executes Jobs.
 * 
 * It has a number of cores, and each core has a certain power measured
 * in MIPS (millions of instructions per second).
 * 
 * It has an input Port that is used to transfer data to the VM, and an output
 * Port that is used to transfer data from the VM. Both ports have the same
 * bandwidth.
 * 
 * Jobs can be queued and are executed in FIFO order. The scheduling is
 * space shared.
 * 
 * It has a price per hour. The cost of a VM is computed by multiplying the
 * runtime in hours by the hourly price. The runtime is rounded up to the
 * nearest hour for this calculation.
 * 
 * Each VM has a provisioning delay between when it is launched and when it
 * is ready, and a deprovisioning delay between when it is terminated and
 * when the provider stops charging for it.
 * 
 * @author Gideon Juve <juve@usc.edu>
 */
public class VM extends CWSSimEntity {

    private static int next_id = 0;

    /** How many seconds there are in one hour */
    public static final double SECONDS_PER_HOUR = 60 * 60;

    /** VM parameters like cores number, price for hour **/
    private VMStaticParams vmStaticParams;

    /** The SimEntity that owns this VM */
    private int owner;

    /** The Cloud that runs this VM */
    private int cloud;

    /**
     * The number of bytes on internal disk that can be used as a cache
     * @see {@link VMCacheManager}
     */
    private long cacheSize = VMFactory.DEFAULT_CACHE_SIZE;

    /** Current idle cores */
    private int idleCores;

    /** Queue of jobs submitted to this VM */
    private LinkedList<Job> jobs;

    /** Set of jobs currently running */
    private Set<Job> runningJobs;

    /** Time that the VM was launched */
    private double launchTime;

    /** Time that the VM was terminated */
    private double terminateTime;

    /** Has this VM been terminated? */
    private boolean isTerminated;

    /** Number of CPU seconds consumed by jobs on this VM */
    private double cpuSecondsConsumed;

    /** Delay from when the VM is launched until it is ready */
    private double provisioningDelay = VMFactory.DEFAULT_PROVISIONING_DELAY;

    /** Delay from when the VM is terminated until it is no longer charged */
    private double deprovisioningDelay = VMFactory.DEFAULT_DEPROVISIONING_DELAY;

    /** Varies the actual runtime of tasks according to the specified distribution */
    private RuntimeDistribution runtimeDistribution = new IdentityRuntimeDistribution();

    /** Varies the failure rate of tasks according to a specified distribution */
    private FailureModel failureModel = new FailureModel(0, 0.0);

    public VM(VMStaticParams vmStaticParams, CloudSimWrapper cloudsim) {
        super("VM" + (next_id++), cloudsim);
        this.vmStaticParams = vmStaticParams;
        this.jobs = new LinkedList<Job>();
        this.runningJobs = new HashSet<Job>();
        this.idleCores = vmStaticParams.getCores();
        this.launchTime = -1.0;
        this.terminateTime = -1.0;
        this.cpuSecondsConsumed = 0.0;
        this.isTerminated = false;
    }

    /**
     * Runtime of the VM in seconds. If the VM has not been launched, then
     * the result is 0. If the VM is not terminated, then we use the current
     * simulation time as the termination time. After the VM is terminated
     * the runtime does not change.
     */
    public double getRuntime() {
        if (launchTime < 0)
            return 0.0;
        else if (terminateTime < 0)
            return getCloudsim().clock() - launchTime;
        else
            return terminateTime - launchTime;
    }

    /**
     * Compute the total cost of this VM. This is computed by taking the
     * runtime, rounding it up to the nearest whole hour, and multiplying
     * by the hourly price.
     */
    public double getCost() {
        double hours = getRuntime() / SECONDS_PER_HOUR;
        hours = Math.ceil(hours);
        // Log.printLine(CloudSim.clock() + " VM " + getId() + " cost " + hours * price);
        return hours * vmStaticParams.getPrice();
    }

    public double getCPUSecondsConsumed() {
        return cpuSecondsConsumed;
    }

    /** cpu_seconds / (runtime * cores) */
    public double getUtilization() {
        double totalCPUSeconds = getRuntime() * vmStaticParams.getCores();
        return cpuSecondsConsumed / totalCPUSeconds;
    }

    @Override
    public void processEvent(CWSSimEvent ev) {
        if (!isTerminated) {
            switch (ev.getTag()) {
            case WorkflowEvent.VM_LAUNCH:
                launchVM();
                break;
            case WorkflowEvent.VM_TERMINATE:
                terminateVM();
                break;
            case WorkflowEvent.JOB_SUBMIT:
                jobSubmit((Job) ev.getData());
                break;
            case WorkflowEvent.JOB_FINISHED:
                jobFinish((Job) ev.getData());
                break;
            case WorkflowEvent.STORAGE_ALL_BEFORE_TRANSFERS_COMPLETED:
                allInputsTrasferred((Job) ev.getData());
                break;
            case WorkflowEvent.STORAGE_ALL_AFTER_TRANSFERS_COMPLETED:
                allOutputsTransferred((Job) ev.getData());
                break;
            default:
                throw new UnknownWorkflowEventException("Unknown event: " + ev);
            }
        }
    }

    private void launchVM() {
        // Reset dynamic state
        jobs.clear();
        idleCores = vmStaticParams.getCores();
        cpuSecondsConsumed = 0.0;
        getCloudsim().log(String.format("VM %d started", getId()));
    }

    private void terminateVM() {
        // Can no longer accept jobs
        isTerminated = true;

        // Move running jobs back to the queue...
        jobs.addAll(runningJobs);
        runningJobs.clear();

        // ... and fail all queued jobs
        for (Job job : jobs) {
            job.setResult(Job.Result.FAILURE);
            getCloudsim().send(getId(), job.getOwner(), 0.0, WorkflowEvent.JOB_FINISHED, job);
            getCloudsim().log("Terminating job " + job.getID() + " on VM " + job.getVM().getId());
        }

        // Reset dynamic state
        jobs.clear();
        idleCores = vmStaticParams.getCores();
        getCloudsim().log(String.format("VM %d terminated", getId()));
    }

    private void jobSubmit(Job job) {
        job.setSubmitTime(getCloudsim().clock());
        job.setState(Job.State.IDLE);
        job.setVM(this);

        // Queue the job
        jobs.add(job);

        // This shouldn't do anything if the VM is busy
        startJobs();
    }

    private void allInputsTrasferred(Job job) {
        // Compute the duration of the job on this VM
        double size = job.getTask().getSize();
        double predictedRuntime = size / vmStaticParams.getMips();

        // Compute actual runtime
        double actualRuntime = this.runtimeDistribution.getActualRuntime(predictedRuntime);

        // Decide whether the job succeeded or failed
        if (failureModel.failureOccurred()) {
            job.setResult(Job.Result.FAILURE);

            // How long did it take to fail?
            actualRuntime = failureModel.runtimeBeforeFailure(actualRuntime);
        } else {
            job.setResult(Job.Result.SUCCESS);
        }

        getCloudsim().log(
                String.format("Starting computational part of job %s (task_id = %s, workflow = %s) on VM %s",
                        job.getID(), job.getTask().getId(), job.getDAGJob().getDAG().getId(), job.getVM().getId()));

        getCloudsim().send(getId(), getId(), actualRuntime, WorkflowEvent.JOB_FINISHED, job);
    }

    private void allOutputsTransferred(Job job) {
        // remove from the running set
        runningJobs.remove(job);

        // Complete the job
        job.setFinishTime(getCloudsim().clock());
        job.setState(Job.State.TERMINATED);

        // Increment the usage
        cpuSecondsConsumed += job.getDuration();

        // Tell the owner
        getCloudsim().send(getId(), job.getOwner(), 0.0, WorkflowEvent.JOB_FINISHED, job);

        // The core that was running the job is now free
        idleCores++;

        // We may be able to start more jobs now
        startJobs();
    }

    private void jobStart(Job job) {
        getCloudsim().log("Starting job " + job.getTask().getId() + " on VM " + job.getVM().getId());
        // The job is now running
        job.setStartTime(getCloudsim().clock());
        job.setState(Job.State.RUNNING);
        // add it to the running set
        runningJobs.add(job);

        // Tell the owner
        getCloudsim().send(getId(), job.getOwner(), 0.0, WorkflowEvent.JOB_STARTED, job);

        getCloudsim().send(getId(), getCloudsim().getEntityId("StorageManager"), 0.0,
                WorkflowEvent.STORAGE_BEFORE_TASK_START, job);

        // One core is now busy running the job
        idleCores--;
    }

    private void jobFinish(Job job) {

        getCloudsim().log(
                String.format(
                        "Computational part of job %s (task_id = %s, workflow = %s, retry = %s) on VM %s finished", job
                                .getID(), job.getTask().getId(), job.getDAGJob().getDAG().getId(), job.isRetry(), job
                                .getVM().getId()));

        getCloudsim().send(getId(), getCloudsim().getEntityId("StorageManager"), 0.0,
                WorkflowEvent.STORAGE_AFTER_TASK_COMPLETED, job);
    }

    private void startJobs() {
        // While there are still idle jobs and cores
        while (jobs.size() > 0 && idleCores > 0) {
            // Start the next job in the queue
            jobStart(jobs.poll());
        }
    }

    public void setDeprovisioningDelay(double deprovisioningDelay) {
        this.deprovisioningDelay = deprovisioningDelay;
    }

    public double getDeprovisioningDelay() {
        return deprovisioningDelay;
    }

    public void setProvisioningDelay(double provisioningDelay) {
        this.provisioningDelay = provisioningDelay;
    }

    public double getProvisioningDelay() {
        return provisioningDelay;
    }

    public int getOwner() {
        return owner;
    }

    public void setOwner(int owner) {
        this.owner = owner;
    }

    public int getCloud() {
        return cloud;
    }

    public void setCloud(int cloud) {
        this.cloud = cloud;
    }

    public int getIdleCores() {
        return idleCores;
    }

    public Job[] getQueuedJobs() {
        return jobs.toArray(new Job[0]);
    }

    public int getQueueLength() {
        return jobs.size();
    }

    public void setLaunchTime(double launchTime) {
        this.launchTime = launchTime;
    }

    public double getLaunchTime() {
        return launchTime;
    }

    public void setTerminateTime(double terminateTime) {
        this.terminateTime = terminateTime;
    }

    public double getTerminateTime() {
        return terminateTime;
    }

    public VMStaticParams getVmStaticParams() {
        return vmStaticParams;
    }

    public RuntimeDistribution getRuntimeDistribution() {
        return runtimeDistribution;
    }

    public void setRuntimeDistribution(RuntimeDistribution runtimeDistribution) {
        this.runtimeDistribution = runtimeDistribution;
    }

    public FailureModel getFailureModel() {
        return failureModel;
    }

    public void setFailureModel(FailureModel failureModel) {
        this.failureModel = failureModel;
    }

    public long getCacheSize() {
        return cacheSize;
    }

    public void setCacheSize(long cacheSize) {
        this.cacheSize = cacheSize;
    }

    public void setVmStaticParams(VMStaticParams vmStaticParams) {
        this.vmStaticParams = vmStaticParams;
    }

    public boolean isTerminated() {
        return isTerminated;
    }

    public void setTerminated(boolean terminated) {
        isTerminated = terminated;
    }
}
