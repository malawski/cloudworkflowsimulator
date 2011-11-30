package cws.core.provisioner;

import cws.core.Cloud;
import cws.core.Provisioner;
import cws.core.WorkflowEvent;

public class AbstractProvisioner {

	protected static final double PROVISIONER_INTERVAL = 90.0;
	protected Cloud cloud;

	public AbstractProvisioner() {
		super();
	}

	public void setCloud(Cloud cloud) {
		this.cloud = cloud;
	}

}