package cws.core.pricing.models;

/**
 * Created by Marcin Ziaber on 2016-10-11.
 */
public abstract class PricingModel {
    /**
     * For how long we pay in advance
     */
    protected final double billingTimeInSeconds;

    public PricingModel(double billingTimeInSeconds) {
        this.billingTimeInSeconds = billingTimeInSeconds;
    }
}
