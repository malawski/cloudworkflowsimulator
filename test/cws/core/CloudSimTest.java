package cws.core;

import org.cloudbus.cloudsim.core.CloudSim;
import org.junit.Before;

/**
 * Should be extended by all classes using CloudSim in tests.
 */
public abstract class CloudSimTest {

    @Before
    public void setUpCloudSim() {
        CloudSim.init(1, null, false);
    }
}
