package cws.core.storage.cache;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

import cws.core.VM;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.dag.DAGFile;
import cws.core.jobs.Job;

/**
 * TODO(bryk): comment it.
 */
public class FIFOVMCacheManager extends VMCacheManager {
    public FIFOVMCacheManager(CloudSimWrapper cloudsim) {
        super(cloudsim);
    }

    private Map<VM, VMCache> cache = new HashMap<VM, FIFOVMCacheManager.VMCache>();

    /**
     * TODO(bryk): comment it.
     */
    private class VMCache {
        private long size = 0;
        private long remainingSize = 0;
        // didn't use LinkedHashSet because it doesn't have push/poll methods
        private LinkedList<DAGFile> filesList = new LinkedList<DAGFile>();
        private Set<DAGFile> filesSet = new HashSet<DAGFile>();

        public VMCache(VM vm) {
            this.size = vm.getCacheSize();
            this.remainingSize = this.size;
        }

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

        public boolean getFileFromCache(DAGFile file) {
            return filesSet.contains(file);
        }
    }

    /**
     * TODO(bryk): comment it.
     */
    @Override
    public void putFileToCache(DAGFile file, Job job) {
        if (cache.get(job.getVM()) == null) {
            cache.put(job.getVM(), new VMCache(job.getVM()));
        }
        cache.get(job.getVM()).putFileToCache(file);
    }

    /**
     * TODO(bryk): comment it.
     */
    @Override
    public boolean getFileFromCache(DAGFile file, Job job) {
        VMCache vmCache = cache.get(job.getVM());
        if (vmCache != null) {
            return vmCache.getFileFromCache(file);
        }
        return false;
    }
}
