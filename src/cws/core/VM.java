package cws.core;

import java.util.*;

import com.google.common.base.Preconditions;

import cws.core.cloudsim.CWSSimEntity;
import cws.core.cloudsim.CWSSimEvent;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.core.VMType;
import cws.core.engine.Environment;
import cws.core.exception.UnknownWorkflowEventException;
import cws.core.jobs.Job;
import cws.core.jobs.RuntimeDistribution;

/**
 * A VM is a virtual machine that executes Jobs.
 * <p>
 * It has a number of cores, and each core has a certain power measured
 * in MIPS (millions of instructions per second).
 * <p>
 * It has an input Port that is used to transfer data to the VM, and an output
 * Port that is used to transfer data from the VM. Both ports have the same
 * bandwidth.
 * <p>
 * Jobs can be queued and are executed in FIFO order. The scheduling is
 * space shared.
 * <p>
 * It has a price per billing unit. The cost of a VM is computed by multiplying the
 * runtime in billing units by the billing unit price. The runtime is rounded up to the
 * nearest billing unit for this calculation.
 * <p>
 * Each VM has a provisioning delay between when it is launched and when it
 * is ready, and a deprovisioning delay between when it is terminated and
 * when the provider stops charging for it.
 *
 * @author Gideon Juve <juve@usc.edu>
 */
public class VM extends CWSSimEntity {

    private static int nextId = 0;

    /**
     * Contains VM parameters like cores number, price for billing unit
     **/
    private final VMType vmType;

    /**
     * The SimEntity that owns this VM
     */
    private int owner = -1;

    /**
     * The Cloud that runs this VM
     */
    private int cloud = -1;

    /**
     * Current idle cores
     */
    private int idleCores;

    /**
     * Queue of jobs submitted to this VM
     */
    private final LinkedList<Job> jobs;

    /**
     * Set of jobs currently running
     */
    private final Set<Job> runningJobs;

    /**
     * Time that the VM was launched
     */
    private double launchTime;

    /**
     * Time that the VM was terminated
     */
    private double terminateTime;

    /**
     * Has this VM been terminated?
     */
    private boolean isTerminated;

    /**
     * Has this VM been started?
     */
    private boolean isLaunched;

    /**
     * Varies the actual runtime of tasks according to the specified distribution
     */
    private final RuntimeDistribution runtimeDistribution;

    /**
     * Varies the failure rate of tasks according to a specified distribution
     */
    private final FailureModel failureModel;

    /**
     * Read intervals of all jobs.
     */
    private final Map<Job, Interval> readIntervals = new HashMap<Job, VM.Interval>();

    /**
     * Write intervals of all jobs.
     */
    private final Map<Job, Interval> writeIntervals = new HashMap<Job, VM.Interval>();

    /**
     * Computation intervals of all jobs.
     */
    private final Map<Job, Interval> computationIntervals = new HashMap<Job, VM.Interval>();

    VM(VMType vmType, CloudSimWrapper cloudsim, FailureModel failureModel, RuntimeDistribution runtimeDistribution) {
        super("VM" + (nextId++), cloudsim);
        this.vmType = vmType;
        this.jobs = new LinkedList<Job>();
        this.runningJobs = new HashSet<Job>();
        this.idleCores = vmType.getCores();
        this.launchTime = -1.0;
        this.terminateTime = -1.0;
        this.isTerminated = false;
        this.isLaunched = false;
        this.failureModel = failureModel;
        this.runtimeDistribution = runtimeDistribution;
    }

    /**
     * Returns true when this VM has at least one idle core.
     */
    public boolean isFree() {
        if (this.isTerminated) {
            throw new IllegalStateException(
                    "Attempted to determine whether terminated VM is free. Check for termination first.");
        }
        return this.idleCores > 0;
    }

    /**
     * Returns true when all of its core are idle.
     */
    public boolean isIdle() {
        if (this.isTerminated) {
            throw new IllegalStateException(
                    "Attempted to determine whether terminated VM is idle. Check for termination first.");
        }
        return this.idleCores == this.vmType.getCores();
    }

    /**
     * Runtime of the VM in seconds. If the VM has not been launched, then the result is 0. If the VM is not terminated,
     * then we use the current simulation time as the termination time. After the VM is terminated the runtime does not
     * change.
     */
    public double getRuntime() {
        if (launchTime < 0)
            return 0.0;
        else if (terminateTime < 0)
            return getCloudsim().clock() - launchTime;
        else
            return terminateTime - launchTime;
    }

    @Override
    public void processEvent(CWSSimEvent ev) {
        if (!isTerminated) {
            switch (ev.getTag()) {
                case WorkflowEvent.VM_LAUNCH:
                    launch();
                    break;
                case WorkflowEvent.VM_TERMINATE:
                    terminate();
                    break;
                case WorkflowEvent.JOB_SUBMIT:
                    jobSubmit((Job) ev.getData());
                    break;
                case WorkflowEvent.JOB_FINISHED:
                    jobFinish((Job) ev.getData());
                    break;
                case WorkflowEvent.STORAGE_ALL_BEFORE_TRANSFERS_COMPLETED:
                    allInputsTransferred((Job) ev.getData());
                    break;
                case WorkflowEvent.STORAGE_ALL_AFTER_TRANSFERS_COMPLETED:
                    allOutputsTransferred((Job) ev.getData());
                    break;
                default:
                    throw new UnknownWorkflowEventException("Unknown event: " + ev);
            }
        } else {
            if (ev.getTag() == WorkflowEvent.VM_LAUNCH || ev.getTag() == WorkflowEvent.JOB_SUBMIT) {
                throw new IllegalStateException(
                        "Attempted to send launch or submit event to terminated VM:" + this.getId());
            }
        }
    }

    /**
     * Launches this VM.
     */
    void launch() {
        Preconditions.checkState(!isLaunched, "Attempted to launch already launched VM:" + this.getId());
        isLaunched = true;
        getCloudsim().log(String.format(Locale.US, "VM %d with %d cores started, and with price %f", getId(), this.vmType.getCores(), this.vmType.getPriceForBillingUnit()));
    }

    /**
     * Terminates this VM. A VM cannot be terminated twice.
     */
    void terminate() {
        Preconditions.checkState(!isTerminated, "Cannot terminate already terminated VM");
        getCloudsim().log("VM " + getId() + " is going to terminate");
        // Can no longer accept jobs
        isTerminated = true;

        // Log termination only for running jobs
        for (Job runningJob : runningJobs) {
            getCloudsim().log("Terminating job " + runningJob.getID() + " on VM " + runningJob.getVM().getId());
        }

        // Log that queued jobs were not executed
        for (Job queuedJob : jobs) {
            getCloudsim().log("Removing job " + queuedJob.getID() + " from queue on VM " + queuedJob.getVM().getId());
        }

        // Move running jobs back to the queue...
        jobs.addAll(runningJobs);
        runningJobs.clear();

        // ... and fail all queued jobs
        for (Job job : jobs) {
            job.setResult(Job.Result.FAILURE);
            getCloudsim().send(getId(), job.getOwner(), 0.0, WorkflowEvent.JOB_FINISHED, job);
        }

        // Reset dynamic state
        jobs.clear();
        idleCores = vmType.getCores();
        getCloudsim().log(String.format("VM %d terminate request success", getId()));
    }

    /**
     * Submits the given job to this VM and decreases number of idle cores.
     */
    public void jobSubmit(Job job) {
        Preconditions.checkState(!isTerminated,
                "Attempted to submit job to a terminated VM. Check for termination first.");
        Preconditions.checkState(isLaunched, "Attempted to submit job to a not launched VM.");
        job.setSubmitTime(getCloudsim().clock());
        job.setState(Job.State.IDLE);
        job.setVM(this);

        // Queue the job
        jobs.add(job);

        // This shouldn't do anything if the VM has no idle cores
        startJobs();
    }

    private void allInputsTransferred(Job job) {
        // Compute the duration of the job on this VM
        double size = job.getTask().getSize();
        double predictedRuntime = size / vmType.getMips();

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

        getCloudsim().log(String.format(
                "Starting computational part of job %s (task_id = %s, workflow = %s) on VM %s. Will finish in %f",
                job.getID(), job.getTask().getId(), job.getDAGJob().getDAG().getId(), job.getVM().getId(),
                actualRuntime));

        getCloudsim().send(getId(), getId(), actualRuntime, WorkflowEvent.JOB_FINISHED, job);

        // Mark that read has finished.
        readIntervals.get(job).stop();
        // Mark that computation has started.
        computationIntervals.put(job, new Interval());
    }

    private void allOutputsTransferred(Job job) {
        if (!runningJobs.contains(job) || job.getState() != Job.State.RUNNING) {
            throw new IllegalStateException("Outputs of non-running job transferred:" + job.getID());
        }
        // remove from the running set
        runningJobs.remove(job);

        // Complete the job
        job.setFinishTime(getCloudsim().clock());
        job.setState(Job.State.TERMINATED);

        // Tell the owner
        getCloudsim().send(getId(), job.getOwner(), 0.0, WorkflowEvent.JOB_FINISHED, job);

        // The core that was running the job is now free
        idleCores++;

        // Mark that write has finished.
        writeIntervals.get(job).stop();

        // We may be able to start more jobs now
        startJobs();
    }

    private void jobStart(final Job job) {
        if (job.getState() != Job.State.IDLE) {
            throw new IllegalStateException("Attempted to start non-idle job:" + job.getID());
        } else if (this.idleCores < 1) {
            throw new IllegalStateException("There are no idle cores in this VM.");
        }
        getCloudsim().log("Starting " + job.toString() + " on VM " + job.getVM().getId());
        // The job is now running
        job.setStartTime(getCloudsim().clock());
        job.setState(Job.State.RUNNING);

        // Tell the owner
        getCloudsim().send(getId(), job.getOwner(), 0.0, WorkflowEvent.JOB_STARTED, job);

        getCloudsim().send(getId(), getCloudsim().getEntityId("StorageManager"), 0.0,
                WorkflowEvent.STORAGE_BEFORE_TASK_START, job);

        // One core is now busy running the job
        this.idleCores--;

        // Mark that read has started.
        this.readIntervals.put(job, new Interval());
        if (this.runningJobs.contains(job)) {
            throw new IllegalStateException("Job already running: " + job);
        }
        // add it to the running set
        this.runningJobs.add(job);
    }

    private void jobFinish(Job job) {
        if (job.getState() != Job.State.RUNNING) {
            throw new RuntimeException("Non-running job finished:" + job.getID());
        }

        String msg = String.format(
                "Computational part of job %s " + "(task_id = %s, workflow = %s, retry = %s) on VM %s finished",
                job.getID(), job.getTask().getId(), job.getDAGJob().getDAG().getId(), job.isRetry(),
                job.getVM().getId());
        getCloudsim().log(msg);

        getCloudsim().send(getId(), getCloudsim().getEntityId("StorageManager"), 0.0,
                WorkflowEvent.STORAGE_AFTER_TASK_COMPLETED, job);

        // Mark that computation has finished
        computationIntervals.get(job).stop();
        // Mark that write has started.
        writeIntervals.put(job, new Interval());
    }

    private void startJobs() {
        // While there are still idle jobs and cores
        while (jobs.size() > 0 && idleCores > 0) {
            // Start the next job in the queue
            jobStart(jobs.poll());
        }
    }

    public int getOwner() {
        if (owner == -1) {
            throw new IllegalStateException();
        }
        return owner;
    }

    public void setOwner(int owner) {
        this.owner = owner;
    }

    public int getCloud() {
        if (cloud == -1) {
            throw new IllegalStateException();
        }
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

    public double getTerminateTime() {
        return terminateTime;
    }

    public void setTerminateTime(double terminateTime) {
        this.terminateTime = terminateTime;
    }

    public VMType getVmType() {
        return vmType;
    }

    public RuntimeDistribution getRuntimeDistribution() {
        return runtimeDistribution;
    }

    public FailureModel getFailureModel() {
        return failureModel;
    }

    public boolean isTerminated() {
        return isTerminated;
    }

    public double getProvisioningDelay() {
        return vmType.getProvisioningDelay().sample();
    }

    public double getDeprovisioningDelay() {
        return vmType.getDeprovisioningDelay().sample();
    }

    /**
     * Assumes one core per one job.
     */
    public double getTimeSpentOnComputations() {
        double time = 0;
        for (Interval interval : computationIntervals.values()) {
            time += interval.getDuration();
        }
        return time;
    }

    /**
     * Assumes one core per one job.
     */
    public double getTimeSpentOnTransfers() {
        double time = 0;
        for (Interval interval : readIntervals.values()) {
            time += interval.getDuration();
        }
        for (Interval interval : writeIntervals.values()) {
            time += interval.getDuration();
        }
        return time;
    }

    /**
     * Represents interval of time in seconds spanning from start time to end time (or VM termination time if not set)
     * or from start time to current CloudSim time if not finished yet.
     */
    private final class Interval {
        private final double startTime = getCloudsim().clock();

        private Double endTime;

        /**
         * Stops the interval at current simulation time.
         */
        public void stop() {
            this.endTime = getCloudsim().clock();
        }

        /**
         * Returns the duration in seconds of this interval.
         */
        public double getDuration() {
            final double duration;
            if (this.endTime == null) {
                if (VM.this.isTerminated) {
                    duration = VM.this.terminateTime - this.startTime;
                } else {
                    duration = getCloudsim().clock() - this.startTime;
                }
            } else {
                duration = this.endTime - this.startTime;
            }
            if (duration < 0) {
                throw new IllegalStateException("Duration is < 0, but shouldn't be");
            }
            return duration;
        }
    }

    /**
     * Returns the time from now when this VM is predicted to have at least one idle core. This executes in the context
     * of {@link Environment}}.
     */
    public double getPredictedReleaseTime(Environment env) {
        final List<Double> taskRuntimes = new ArrayList<Double>();
        for (final Job job : this.runningJobs) {
            taskRuntimes.add(getPredictedRemainingRuntime(job, env));
        }
        for (final Job job : this.jobs) {
            taskRuntimes.add(getPredictedRemainingRuntime(job, env));
        }
        final Double predictedReleaseTime = calculatePredictedReleaseTime(taskRuntimes);
        // If predicted time is < 0 then return zero not to be better than free VMs.
        return predictedReleaseTime > 0 ? predictedReleaseTime : 0;
    }

    private double getPredictedRemainingRuntime(final Job job, final Environment env) {
        if (this.writeIntervals.containsKey(job)) { // job is in output files transfer phase
            return env.getOutputTransferTimeEstimation(job.getTask(), this)
                    - this.writeIntervals.get(job).getDuration();
        } else if (this.computationIntervals.containsKey(job)) { // job is in computation phase
            return env.getOutputTransferTimeEstimation(job.getTask(), this)
                    + env.getComputationPredictedRuntimeForSingleTask(this.vmType, job.getTask())
                    - this.computationIntervals.get(job).getDuration();
        } else if (this.readIntervals.containsKey(job)) { // job is in input files transfer phase
            return env.getTotalTransferTimeEstimation(job.getTask(), this) - this.readIntervals.get(job).getDuration()
                    + env.getComputationPredictedRuntimeForSingleTask(this.vmType, job.getTask());
        } else { // job is waiting in queue
            return env.getTotalTransferTimeEstimation(job.getTask(), this)
                    + env.getComputationPredictedRuntimeForSingleTask(this.vmType, job.getTask());
        }
    }

    // Given list of scheduled tasks' runtimes calculates when at least one core of this vm will become idle
    private double calculatePredictedReleaseTime(final List<Double> taskRuntimes) {
        if (taskRuntimes.size() < this.vmType.getCores()) {
            return 0.0;
        }
        final PriorityQueue<Double> queue = new PriorityQueue<Double>();
        final Iterator<Double> iterator = taskRuntimes.iterator();
        for (int i = 0; i < this.vmType.getCores(); i++) {
            queue.add(iterator.next());
        }
        while (iterator.hasNext()) {
            queue.add(queue.poll() + iterator.next());
        }
        return queue.poll();
    }
}
