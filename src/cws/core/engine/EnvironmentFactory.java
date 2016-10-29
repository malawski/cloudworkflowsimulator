package cws.core.engine;

import cws.core.cloudsim.CloudSimWrapper;
import cws.core.core.VMType;
import cws.core.pricing.PricingManager;
import cws.core.pricing.PricingModelFactory;
import cws.core.simulation.StorageSimulationParams;
import cws.core.storage.StorageManager;
import cws.core.storage.StorageManagerFactory;

import java.util.Map;
import java.util.Set;

/**
 * A factory which creates {@link Environment} instances.
 */
public class EnvironmentFactory {
    /**
     * Creates new {@link Environment} instance. It uses mips(1).cores(1).price(1.0) hadrcoded default values for
     * {@link VMType}.
     * 
     * @param cloudsim Initialized {@link CloudSimWrapper} instance.
     * @param simulationParams Params for {@link StorageManagerFactory}.
     * @return Newly created {@link Environment} instance.
     */
    public static Environment createEnvironment(CloudSimWrapper cloudsim, StorageSimulationParams simulationParams,
            Map<String, Object> pricingConfig, Set<VMType> vmTypes) {
        StorageManager storageManager = StorageManagerFactory.createStorage(simulationParams, cloudsim);
        PricingManager pricingManager = new PricingManager(PricingModelFactory.getPricingModel(pricingConfig));
        return new Environment(vmTypes, storageManager, pricingManager);
    }
}
