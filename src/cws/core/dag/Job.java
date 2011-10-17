package cws.core.dag;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

import org.cloudbus.cloudsim.Cloudlet;

public class Job {

	private List<Cloudlet> cloudlets;
	
	private HashMap<Cloudlet,Node> nodesMap;
	
	private Node fanInNode;
	private Node fanOutNode;
	private Random random = new Random();

	
	public List<Cloudlet> getCloudlets() {
		return cloudlets;
	}
	public void setCloudlets(List<Cloudlet> cloudlets) {
		this.cloudlets = cloudlets;
	}
	public Node getFanInNode() {
		return fanInNode;
	}
	public void setFanInNode(Node fanInNode) {
		this.fanInNode = fanInNode;
	}
	public Node getFanOutNode() {
		return fanOutNode;
	}
	public void setFanOutNode(Node fanOutNode) {
		this.fanOutNode = fanOutNode;
	}
	
	public void generateDag() {
		
		nodesMap = new HashMap<Cloudlet, Node>();
		
		ArrayList<Node> added = new ArrayList<Node>();
		ArrayList<Cloudlet> remaining = new ArrayList<Cloudlet>();
		remaining.addAll(cloudlets);
		fanInNode = new Node();
		Cloudlet cloudlet0 = remaining.remove(0);
		fanInNode.setId(cloudlet0.getCloudletId());
		added.add(fanInNode);
		nodesMap.put(cloudlet0, fanInNode);
		fanInNode.setEligible(true);
		
		for (Cloudlet cloudlet : remaining) {
			Node node = new Node();
			node.setId(cloudlet.getCloudletId());
			Node parent = added.get(random.nextInt(added.size()));
			node.addParent(parent);
			added.add(node);
			nodesMap.put(cloudlet, node);
		}
		fanInNode.print("");
	}
	
	/* TODO for efficiency reasons it might be better to store eligible list instead of regenerating it */
	
	public ArrayList<Cloudlet> getEligibleCloudlets() {
		ArrayList<Cloudlet> eligibleCloudlets = new ArrayList<Cloudlet>();
		for (Cloudlet cloudlet : cloudlets) {
			if (nodesMap.get(cloudlet).isEligible()) eligibleCloudlets.add(cloudlet);
		}
		return eligibleCloudlets;
	}
	
	public void setUneligible(Cloudlet cloudlet) {
		nodesMap.get(cloudlet).setEligible(false);
	}
	
	public void processCloudletReturn(Cloudlet cloudlet) {
		Node node = nodesMap.get(cloudlet);
		node.setDone(true);
		node.updateChildren();
	}
	
}
