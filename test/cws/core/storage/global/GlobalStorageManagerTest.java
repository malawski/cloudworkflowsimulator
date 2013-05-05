package cws.core.storage.global;

import org.junit.Before;

import cws.core.storage.StorageManagerTest;

/**
 * Tests {@link GlobalStorageManager}
 */
public class GlobalStorageManagerTest extends StorageManagerTest {

    @Before
    public void setUp() {
        storageManager = new GlobalStorageManager(100, 200, cloudsim);
    }
}
