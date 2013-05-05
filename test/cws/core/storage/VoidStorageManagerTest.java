package cws.core.storage;

import org.junit.Before;

/**
 * Tests {@link VoidStorageManager}.
 */
public class VoidStorageManagerTest extends StorageManagerTest {

    @Before
    public void setUp() {
        storageManager = new VoidStorageManager(cloudsim);
    }
}
