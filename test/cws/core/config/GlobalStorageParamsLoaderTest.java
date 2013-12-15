package cws.core.config;

import static junit.framework.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import cws.core.exception.IllegalCWSArgumentException;
import cws.core.storage.global.GlobalStorageParams;

public class GlobalStorageParamsLoaderTest {

    private GlobalStorageParamsLoader loader;
    private Map<String, Object> config;

    @Before
    public void setUp() throws Exception {
        loader = new GlobalStorageParamsLoader();
        createValidConfig();
    }

    private void createValidConfig() {
        config = new HashMap<String, Object>();
        config.put(GlobalStorageParamsLoader.GS_READ_SPEED_CONFIG_ENTRY, 1.0);
        config.put(GlobalStorageParamsLoader.GS_WRITE_SPEED_CONFIG_ENTRY, 1.0);
        config.put(GlobalStorageParamsLoader.GS_LATENCY_CONFIG_ENTRY, 0.01);
        config.put(GlobalStorageParamsLoader.GS_REPLICAS_NUMBER_CONFIG_ENTRY, 1);
        config.put(GlobalStorageParamsLoader.GS_CHUNK_TRANSFER_TIME_CONFIG_ENTRY, 3.2);
    }

    @Test
    public void shouldLoadReadSpeed() {
        config.put(GlobalStorageParamsLoader.GS_READ_SPEED_CONFIG_ENTRY, 1234.4);

        GlobalStorageParams globalStorageParams = loader.loadParams(config);

        assertEquals(1234.4, globalStorageParams.getReadSpeed());
    }

    @Test(expected = IllegalCWSArgumentException.class)
    public void shouldFailIfReadSpeedIsMissing() {
        config.remove(GlobalStorageParamsLoader.GS_WRITE_SPEED_CONFIG_ENTRY);

        loader.loadParams(config);
    }

    @Test(expected = IllegalCWSArgumentException.class)
    public void shouldFailIfReadSpeedIsNotANumber() {
        config.put(GlobalStorageParamsLoader.GS_READ_SPEED_CONFIG_ENTRY, "invalid");

        loader.loadParams(config);
    }

    @Test(expected = IllegalCWSArgumentException.class)
    public void shouldFailIfReadSpeedIsLessThanZero() {
        config.put(GlobalStorageParamsLoader.GS_READ_SPEED_CONFIG_ENTRY, -123.0);

        loader.loadParams(config);
    }

    @Test(expected = IllegalCWSArgumentException.class)
    public void shouldFailIfReadSpeedIsZero() {
        config.put(GlobalStorageParamsLoader.GS_READ_SPEED_CONFIG_ENTRY, 0.0);

        loader.loadParams(config);
    }

    @Test
    public void shouldLoadWriteSpeed() {
        config.put(GlobalStorageParamsLoader.GS_WRITE_SPEED_CONFIG_ENTRY, 321.4);

        GlobalStorageParams globalStorageParams = loader.loadParams(config);

        assertEquals(321.4, globalStorageParams.getWriteSpeed());
    }

    @Test(expected = IllegalCWSArgumentException.class)
    public void shouldFailIfWriteSpeedIsMissing() {
        config.remove(GlobalStorageParamsLoader.GS_WRITE_SPEED_CONFIG_ENTRY);

        loader.loadParams(config);
    }

    @Test(expected = IllegalCWSArgumentException.class)
    public void shouldFailIfWriteSpeedIsNotANumber() {
        config.put(GlobalStorageParamsLoader.GS_WRITE_SPEED_CONFIG_ENTRY, "invalid");

        loader.loadParams(config);
    }

    @Test(expected = IllegalCWSArgumentException.class)
    public void shouldFailIfWriteSpeedIsLessThanZero() {
        config.put(GlobalStorageParamsLoader.GS_WRITE_SPEED_CONFIG_ENTRY, -123.0);

        loader.loadParams(config);
    }

    @Test(expected = IllegalCWSArgumentException.class)
    public void shouldFailIfWriteSpeedIsZero() {
        config.put(GlobalStorageParamsLoader.GS_WRITE_SPEED_CONFIG_ENTRY, 0.0);

        loader.loadParams(config);
    }

    @Test
    public void shouldLoadLatency() {
        config.put(GlobalStorageParamsLoader.GS_LATENCY_CONFIG_ENTRY, 0.0013);

        GlobalStorageParams params = loader.loadParams(config);

        assertEquals(0.0013, params.getLatency());
    }

    @Test(expected = IllegalCWSArgumentException.class)
    public void shouldFailIfLatencyIsMissing() {
        config.remove(GlobalStorageParamsLoader.GS_LATENCY_CONFIG_ENTRY);

        loader.loadParams(config);
    }

    @Test(expected = IllegalCWSArgumentException.class)
    public void shouldFailIfLatencyIsNotANumber() {
        config.put(GlobalStorageParamsLoader.GS_LATENCY_CONFIG_ENTRY, "invalid");

        loader.loadParams(config);
    }

    @Test(expected = IllegalCWSArgumentException.class)
    public void shouldFailIfLatencyIsLessThanZero() {
        config.put(GlobalStorageParamsLoader.GS_LATENCY_CONFIG_ENTRY, -123.0);

        loader.loadParams(config);
    }

    @Test
    public void shouldAcceptLatencyEqualToZero() {
        config.put(GlobalStorageParamsLoader.GS_LATENCY_CONFIG_ENTRY, 0.0);

        GlobalStorageParams params = loader.loadParams(config);

        assertEquals(0.0, params.getLatency());
    }

    @Test
    public void shouldLoadChunkTransferTime() {
        config.put(GlobalStorageParamsLoader.GS_CHUNK_TRANSFER_TIME_CONFIG_ENTRY, 0.0013);

        GlobalStorageParams params = loader.loadParams(config);

        assertEquals(0.0013, params.getChunkTransferTime());
    }

    @Test(expected = IllegalCWSArgumentException.class)
    public void shouldFailIfChunkTransferTimeIsMissing() {
        config.remove(GlobalStorageParamsLoader.GS_CHUNK_TRANSFER_TIME_CONFIG_ENTRY);

        loader.loadParams(config);
    }

    @Test(expected = IllegalCWSArgumentException.class)
    public void shouldFailIfChunkTransferTimeIsNotANumber() {
        config.put(GlobalStorageParamsLoader.GS_CHUNK_TRANSFER_TIME_CONFIG_ENTRY, "invalid");

        loader.loadParams(config);
    }

    @Test(expected = IllegalCWSArgumentException.class)
    public void shouldFailIfChunkTransferTimeIsLessThanZero() {
        config.put(GlobalStorageParamsLoader.GS_CHUNK_TRANSFER_TIME_CONFIG_ENTRY, -123.0);

        loader.loadParams(config);
    }

    @Test(expected = IllegalCWSArgumentException.class)
    public void shouldFailIfChunkTransferTimeIsZero() {
        config.put(GlobalStorageParamsLoader.GS_CHUNK_TRANSFER_TIME_CONFIG_ENTRY, 0.0);

        loader.loadParams(config);
    }

    @Test
    public void shouldLoadReplicasNumber() {
        config.put(GlobalStorageParamsLoader.GS_REPLICAS_NUMBER_CONFIG_ENTRY, 3);

        GlobalStorageParams params = loader.loadParams(config);

        assertEquals(3, params.getNumReplicas());
    }

}
