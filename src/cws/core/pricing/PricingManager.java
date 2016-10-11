package cws.core.pricing;

import cws.core.pricing.models.PricingModel;
import org.apache.commons.cli.CommandLine;

import java.util.Map;

/**
 * Created by Marcin Ziaber on 2016-10-09.
 */
public class PricingManager {

    private PricingModel pricingModel;

    public void loadPricingModel(CommandLine args) {
        PricingConfigLoader pricingConfigLoader = new PricingConfigLoader();
        Map<String, Object> pricingConfig = pricingConfigLoader.loadPricingModel(args);
        final PricingModelFactory pricingModelFactory = new PricingModelFactory();
        this.pricingModel = pricingModelFactory.getPricingModel(pricingConfig);
    }

    @Override
    public String toString() {
        return "PricingManager{" +
                "pricingModel=" + pricingModel +
                '}';
    }
}
