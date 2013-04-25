/**
 * 
 */
package cws.core.storage;

import org.junit.Before;

/**
 * @author piotr
 * 
 */
public class VoidStorageManagerTest extends StorageManagerTest {

    @Before
    public void setUp() {
        storageManager = new VoidStorageManager(cloudsim);
    }
}
