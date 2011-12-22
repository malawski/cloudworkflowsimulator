package cws.core.provisioner;

import cws.core.Provisioner;

public abstract class AbstractProvisioner implements Provisioner {

    protected static final double PROVISIONER_INTERVAL = 90.0;
    
    // maximum autoscaling factor over initial number of provisioned VMs 
    protected double maxScaling = 2.0;
    
    public AbstractProvisioner() {
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
}
