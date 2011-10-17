package cws.core.broker;



import org.cloudbus.cloudsim.DatacenterBroker;

public class BrokerFactory {

	

	/**
	 * Creates the broker.
	 *
	 * @return the datacenter broker
	 */
	public static DatacenterBroker createBroker() {
		DatacenterBroker broker = null;
		try {
			broker = new DatacenterBroker("Broker");
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
		return broker;
	}

	public static DatacenterBroker createBrokerRandom() {
		DatacenterBroker broker = null;
		try {
			broker = new DatacenterBrokerRandom("Broker");
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
		return broker;
	}

	public static DatacenterBroker createBrokerRandomLimited() {
		DatacenterBroker broker = null;
		try {
			broker = new DatacenterBrokerRandomLimited("Broker");
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
		return broker;
	}

	public static DatacenterBrokerRandomDAG createBrokerRandomDAG() {
		DatacenterBrokerRandomDAG broker = null;
		try {
			broker = new DatacenterBrokerRandomDAG("Broker");
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
		return broker;
	}
	
	
}
