package cws.core;

/**
 * An interface for resource provisioners used by the WorkflowEngine.
 *
 * @author Gideon Juve <juve@usc.edu>
 */
public interface Provisioner {
    public void provisionResources(WorkflowEngine engine);
	public void setCloud(Cloud cloud);
	public abstract double getMax_scaling();
	public abstract void setMax_scaling(double max_scaling);
}
