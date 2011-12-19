package cws.core.provisioner;

import cws.core.Cloud;
import cws.core.Provisioner;

public abstract class AbstractProvisioner implements Provisioner {

	protected static final double PROVISIONER_INTERVAL = 90.0;
	protected Cloud cloud;
	
	// maximum autoscaling factor over initial number of provisioned VMs 
	protected double max_scaling = 2.0;

	public AbstractProvisioner() {
		super();
	}

	public void setCloud(Cloud cloud) {
		this.cloud = cloud;
	}

	@Override
	public void setMax_scaling(double max_scaling) {
		this.max_scaling = max_scaling;
	}

	@Override
	public double getMax_scaling() {
		return max_scaling;
	}

}