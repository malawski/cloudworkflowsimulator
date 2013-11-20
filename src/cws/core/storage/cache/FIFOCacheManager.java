package cws.core.storage.cache;

import java.util.*;

import cws.core.VM;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.dag.DAGFile;
import cws.core.jobs.Job;

/**
 * Cache manager which uses FIFO cache strategy for all files.
 */
public class FIFOCacheManager extends VMCacheManager {
    public FIFOCacheManager(CloudSimWrapper cloudsim) {
        super(cloudsim);
    }

    private Map<VM, VMCache> cache = new HashMap<VM, FIFOCacheManager.VMCache>();

    /**
     * Since we use per-VM cache this inner class is convenient.
     */
    private class VMCache {
        private long size = 0;
        private long remainingSize = 0;
        // didn't use LinkedHashSet because it doesn't have push/poll methods
        private LinkedList<DAGFile> filesList = new LinkedList<DAGFile>();
        private Set<DAGFile> filesSet = new HashSet<DAGFile>();

        public VMCache(VM vm) {
            this.size = vm.getVmType().getCacheSize();
            this.remainingSize = this.size;
        }

        /**
         * Puts the file to the local cache.
         */
        public void putFileToCache(DAGFile file) {
            if (file.getSize() <= size) {
                while (remainingSize < file.getSize() && filesSet.size() > 0) {
                    DAGFile df = filesList.pollLast();
                    filesSet.remove(df);
                    remainingSize += df.getSize();
                }
                if (remainingSize >= file.getSize()) {
                    filesSet.add(file);
                    filesList.push(file);
                    remainingSize -= file.getSize();
                }
            }
        }

        /**
         * @return true if the file is in the cache, false otherwise.
         */
        public boolean getFileFromCache(DAGFile file) {
            return filesSet.contains(file);
        }
    }

    @Override
    public void putFileToCache(DAGFile file, Job job) {
        if (cache.get(job.getVM()) == null) {
            cache.put(job.getVM(), new VMCache(job.getVM()));
        }
        cache.get(job.getVM()).putFileToCache(file);
    }

    @Override
    public boolean getFileFromCache(DAGFile file, Job job) {
        VMCache vmCache = cache.get(job.getVM());
        if (vmCache != null) {
            return vmCache.getFileFromCache(file);
        }
        return false;
    }
}
