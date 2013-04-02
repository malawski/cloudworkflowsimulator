package cws.core.provisioner;

import cws.core.Cloud;
import cws.core.Provisioner;

public abstract class AbstractProvisioner implements Provisioner {

    protected static final double PROVISIONER_INTERVAL = 90.0;

    public static final double DEFAULT_AUTOSCALING_FACTOR = 2.0;

    protected Cloud cloud;

    // maximum autoscaling factor over initial number of provisioned VMs
    protected double maxScaling;

    public AbstractProvisioner() {
        this(DEFAULT_AUTOSCALING_FACTOR);
    }

    public AbstractProvisioner(double maxScaling) {
        this.maxScaling = maxScaling;
    }

    public void setMaxScaling(double maxScaling) {
        this.maxScaling = maxScaling;
    }

    public double getMaxScaling() {
        return maxScaling;
    }

    @Override
    public void setCloud(Cloud cloud) {
        this.cloud = cloud;
    }

    public Cloud getCloud() {
        return cloud;
    }
}
