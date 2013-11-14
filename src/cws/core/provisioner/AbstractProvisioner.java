package cws.core.provisioner;

import cws.core.Cloud;
import cws.core.Provisioner;
import cws.core.cloudsim.CloudSimWrapper;

public abstract class AbstractProvisioner implements Provisioner {
    public static final double DEFAULT_AUTOSCALING_FACTOR = 2.0;
    protected static final double PROVISIONER_INTERVAL = 90.0;

    private CloudSimWrapper cloudsim;
    protected Cloud cloud;

    // maximum autoscaling factor over initial number of provisioned VMs
    protected double maxScaling;

    public AbstractProvisioner(CloudSimWrapper cloudsim) {
        this(DEFAULT_AUTOSCALING_FACTOR, cloudsim);
    }

    public AbstractProvisioner(double maxScaling, CloudSimWrapper cloudsim) {
        this.maxScaling = maxScaling;
        this.cloudsim = cloudsim;
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

    protected CloudSimWrapper getCloudSim() {
        return cloudsim;
    }
}
