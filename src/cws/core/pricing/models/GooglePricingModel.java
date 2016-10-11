package cws.core.pricing.models;

/**
 * Created by Marcin Ziaber on 2016-10-09.
 * <p>
 * Pricing model used by Google. First we pay in advance for defined long period of time,
 * and then we switch to the simple model of paying periodically for shorter period of time.
 */
public class GooglePricingModel extends PricingModel {
    /**
     * For how long we pay in advance for first time
     */
    private final double firstBillingTimeInSeconds;


    public GooglePricingModel(double billingTimeInSeconds, double firstBillingTimeInSeconds) {
        super(billingTimeInSeconds);
        this.firstBillingTimeInSeconds = firstBillingTimeInSeconds;
    }

    @Override
    public String toString() {
        return "GooglePricingModel billingTime: " + billingTimeInSeconds + ", firstBillingTime: " + firstBillingTimeInSeconds;
    }
}
