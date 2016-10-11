package cws.core.pricing.models;

/**
 * Created by Marcin Ziaber on 2016-10-09.
 * <p>
 * Pricing model in which we pay periodically in advance for defined in config period of time.
 */
public class SimplePricingModel extends PricingModel {

    public SimplePricingModel(double billingTimeInSeconds) {
        super(billingTimeInSeconds);
    }

    @Override
    public String toString() {
        return "SimplePricingModel billingTime:" + billingTimeInSeconds;
    }
}