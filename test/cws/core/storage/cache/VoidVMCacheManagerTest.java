package cws.core.storage.cache;

import org.junit.Before;

/**
 * Tests for {@link VoidCacheManager}
 */
public class VoidVMCacheManagerTest extends VMCacheManagerTest {
    @Before
    public void setUp() {
        cm = new VoidCacheManager(cloudsim);
    }
}
