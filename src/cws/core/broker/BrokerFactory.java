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
			throw new RuntimeException(e);
		}
		return broker;
	}

	public static DatacenterBroker createBrokerRandom() {
		DatacenterBroker broker = null;
		try {
			broker = new DatacenterBrokerRandom("Broker");
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		return broker;
	}

	public static DatacenterBroker createBrokerRandomLimited(int maxInStage) {
		DatacenterBroker broker = null;
		try {
			broker = new DatacenterBrokerRandomLimited("Broker", maxInStage);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		return broker;
	}

	public static DatacenterBrokerRandomDAG createBrokerRandomDAG() {
		DatacenterBrokerRandomDAG broker = null;
		try {
			broker = new DatacenterBrokerRandomDAG("Broker");
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		return broker;
	}
	
	public static DatacenterBrokerRandomLimitedDAG createBrokerRandomLimitedDAG(int maxInStage) {
		DatacenterBrokerRandomLimitedDAG broker = null;
		try {
			broker = new DatacenterBrokerRandomLimitedDAG("Broker", maxInStage);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		return broker;
	}
	
	
}
