package cws.core.storage.cache;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import cws.core.dag.DAGFile;

/**
 * Tests for {@link FIFOCacheManager}
 */
public class FIFOVMCacheManagerTest extends VMCacheManagerTest {
    @Before
    public void setUp() {
        cm = new FIFOCacheManager(cloudsim);
    }

    @Test
    public void shouldCacheOneFile() {
        Mockito.when(vmType.getCacheSize()).thenReturn((long) 100);
        DAGFile df = new DAGFile("xxxxxx", 100);
        cm.putFileToCache(df, job);
        Assert.assertTrue(cm.getFileFromCache(df, job));
    }

    @Test
    public void shouldEvictOldCacheEntries() {
        int cs = 1000;
        Mockito.when(vmType.getCacheSize()).thenReturn((long) cs);
        int sz = 79;
        int ndags = 30;
        DAGFile[] dfs = new DAGFile[ndags];
        for (int i = 0; i < ndags; i++) {
            DAGFile df = new DAGFile("xxxxxx" + i, sz);
            dfs[i] = df;
            cm.putFileToCache(df, job);
        }
        int numberOfLeftFiles = cs / sz;
        for (int i = 0; i < ndags; i++) {
            if (i >= ndags - numberOfLeftFiles) {
                Assert.assertTrue(cm.getFileFromCache(dfs[i], job));
            } else {
                Assert.assertFalse(cm.getFileFromCache(dfs[i], job));
            }
        }
    }

    @Test
    public void shouldNotAddAndEvictOnTooBigFile() {
        Mockito.when(vmType.getCacheSize()).thenReturn((long) 100);
        DAGFile df = new DAGFile("xxxxxx", 20);
        cm.putFileToCache(df, job);
        DAGFile dfBig = new DAGFile("xxxxxx222", 101);
        cm.putFileToCache(dfBig, job);
        Assert.assertTrue(cm.getFileFromCache(df, job));
        Assert.assertFalse(cm.getFileFromCache(dfBig, job));
    }
}
