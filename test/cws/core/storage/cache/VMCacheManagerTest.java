package cws.core.storage.cache;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import cws.core.VM;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.core.VMType;
import cws.core.dag.DAGFile;
import cws.core.dag.Task;
import cws.core.jobs.Job;

/**
 * Abstract tests for {@link VMCacheManager}
 */
public abstract class VMCacheManagerTest {
    protected VMCacheManager cm;
    protected CloudSimWrapper cloudsim;
    protected Job job;
    protected VM vm;
    protected VMType vmType;
    protected Task task;

    @Before
    public void setUpVMCacheTest() {
        cloudsim = Mockito.spy(new CloudSimWrapper());
        cloudsim.init();

        job = Mockito.mock(Job.class);
        vm = Mockito.mock(VM.class);
        vmType = Mockito.mock(VMType.class);
        Mockito.when(vm.getId()).thenReturn(100);
        Mockito.when(vm.getVmType()).thenReturn(vmType);
        job.setVM(vm);
        Mockito.when(job.getVM()).thenReturn(vm);
        task = Mockito.mock(Task.class);
        Mockito.when(job.getTask()).thenReturn(task);
    }

    @Test
    public void testFilesNotInCache() {
        Assert.assertFalse(cm.getFileFromCache(new DAGFile("abc.txt", 100, null), job));
        Assert.assertFalse(cm.getFileFromCache(new DAGFile("abc.txt", 100, null), job));
        Assert.assertFalse(cm.getFileFromCache(new DAGFile("sadsadsaddsa", 100, null), job));
    }

    @Test
    public void testTooBigFile() {
        Mockito.when(vmType.getCacheSize()).thenReturn((long) 100);
        DAGFile file = new DAGFile("abc.txt", 10000, null);
        cm.putFileToCache(file, job);
        Assert.assertFalse(cm.getFileFromCache(file, job));
    }
}
