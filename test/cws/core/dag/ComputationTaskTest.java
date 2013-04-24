package cws.core.dag;

import org.junit.Before;

/**
 * Tests {@link ComputationTask}
 */
public class ComputationTaskTest extends TaskTest {
    @Override
    @Before
    public void setUpTaskAndJob() {
        task = new ComputationTask("test", ":::", 100);
    }
}
