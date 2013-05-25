package cws.core.storage.cache;

import cws.core.VM;
import cws.core.cloudsim.CWSSimEntity;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.dag.DAGFile;
import cws.core.jobs.Job;

/**
 * Cache manager. It is intended to store ("cache") input and output files in VM's cache space. <br>
 * 
 * Interface contracts:
 * <ul>
 * <li>{@link #getFileFromCache(DAGFile, Job)} can return true if and only if {@link #putFileToCache(DAGFile, Job)} was
 * called before with the same arguments.</li>
 * <li>Files bigger than VM's cache cannot be put into it.</li>
 * </ul>
 * @see {@link VM#getCacheSize()}
 */
public abstract class VMCacheManager extends CWSSimEntity {
    public VMCacheManager(CloudSimWrapper cloudsim) {
        super("VMCacheManager", cloudsim);
    }

    /**
     * Instructs the manager to put the file to the cache. This is only suggestion and implementations are free to
     * decide what to do.<br>
     * Check interface contracts for this method's contracts.
     * @param file - the file to put in the cache.
     * @param job - the file's job.
     */
    public abstract void putFileToCache(DAGFile file, Job job);

    /**
     * Check interface contracts for this method's contracts.
     * @param file - the file to put in the cache.
     * @param job - the file's job.
     * @return true if the file is in the cache, false otherwise.
     */
    public abstract boolean getFileFromCache(DAGFile file, Job job);
}
