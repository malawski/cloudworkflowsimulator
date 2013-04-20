package cws.core.cloudsim;

import org.cloudbus.cloudsim.core.SimEvent;

public class CWSSimEvent {
    private SimEvent simEvent;

    public CWSSimEvent(SimEvent simEvent) {
        this.simEvent = simEvent;
    }

    /**
     * @return
     * @see org.cloudbus.cloudsim.core.SimEvent#toString()
     */
    public String toString() {
        return simEvent.toString();
    }

    /**
     * @return
     * @see org.cloudbus.cloudsim.core.SimEvent#getType()
     */
    public int getType() {
        return simEvent.getType();
    }

    /**
     * @param event
     * @return
     * @see org.cloudbus.cloudsim.core.SimEvent#compareTo(org.cloudbus.cloudsim.core.SimEvent)
     */
    public int compareTo(SimEvent event) {
        return simEvent.compareTo(event);
    }

    /**
     * @return
     * @see org.cloudbus.cloudsim.core.SimEvent#getDestination()
     */
    public int getDestination() {
        return simEvent.getDestination();
    }

    /**
     * @return
     * @see org.cloudbus.cloudsim.core.SimEvent#getSource()
     */
    public int getSource() {
        return simEvent.getSource();
    }

    /**
     * @return
     * @see org.cloudbus.cloudsim.core.SimEvent#eventTime()
     */
    public double eventTime() {
        return simEvent.eventTime();
    }

    /**
     * @return
     * @see org.cloudbus.cloudsim.core.SimEvent#endWaitingTime()
     */
    public double endWaitingTime() {
        return simEvent.endWaitingTime();
    }

    /**
     * @return
     * @see org.cloudbus.cloudsim.core.SimEvent#type()
     */
    public int type() {
        return simEvent.type();
    }

    /**
     * @return
     * @see org.cloudbus.cloudsim.core.SimEvent#scheduledBy()
     */
    public int scheduledBy() {
        return simEvent.scheduledBy();
    }

    /**
     * @return
     * @see org.cloudbus.cloudsim.core.SimEvent#getTag()
     */
    public int getTag() {
        return simEvent.getTag();
    }

    /**
     * @return
     * @see org.cloudbus.cloudsim.core.SimEvent#getData()
     */
    public Object getData() {
        return simEvent.getData();
    }

    /**
     * @return
     * @see org.cloudbus.cloudsim.core.SimEvent#clone()
     */
    public Object clone() {
        return simEvent.clone();
    }

    /**
     * @param s
     * @see org.cloudbus.cloudsim.core.SimEvent#setSource(int)
     */
    public void setSource(int s) {
        simEvent.setSource(s);
    }

    /**
     * @param d
     * @see org.cloudbus.cloudsim.core.SimEvent#setDestination(int)
     */
    public void setDestination(int d) {
        simEvent.setDestination(d);
    }

}
