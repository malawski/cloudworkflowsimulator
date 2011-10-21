package cws.core.dag;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.UtilizationModel;
import org.cloudbus.cloudsim.UtilizationModelFull;

public class Job {

	private List<Cloudlet> cloudlets;
	
	private HashMap<Cloudlet,Node> nodesMap;
	
	private Node fanInNode;
	private Node fanOutNode;
	private Random random = new Random();

	private HashMap<Task, Cloudlet> tasksMap;

	
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
		fanInNode.setCloudletId(cloudlet0.getCloudletId());
		added.add(fanInNode);
		nodesMap.put(cloudlet0, fanInNode);
		fanInNode.setEligible(true);
		
		for (Cloudlet cloudlet : remaining) {
			Node node = new Node();
			node.setCloudletId(cloudlet.getCloudletId());
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
	
	public void readDag(int brokerId, String fileName) {
        DAG dag = DAGParser.parseDAG(new File(fileName));
        cloudlets = new ArrayList<Cloudlet>();
        nodesMap = new HashMap<Cloudlet, Node>();
        tasksMap = new HashMap<Task, Cloudlet>();
        String[] tasks = dag.getTasks();
    	UtilizationModel utilizationModel = new UtilizationModelFull();
    	
        for (int i=0;i<tasks.length;i++) {
        	Task task = dag.getTask(tasks[i]);
        	long mi = (long) task.size * 1000; // we assume that the execution times in seconds are measured on 1000 MIPS processors 
        	Cloudlet cloudlet = new Cloudlet(i, mi , 1, 100, 100, utilizationModel, utilizationModel, utilizationModel);
    		cloudlet.setUserId(brokerId);
        	cloudlets.add(cloudlet);
        	tasksMap.put(task, cloudlet);
        	Node node = new Node();
        	node.setCloudletId(cloudlet.getCloudletId());
        	nodesMap.put(cloudlet, node);
        }
        
        for (int i=0;i<tasks.length;i++) {
        	Task task = dag.getTask(tasks[i]);
        	Node node = nodesMap.get(tasksMap.get(task));
        	List<Task> parents = task.parents;
        	for (Task parent : parents) {
        		node.addParent(nodesMap.get(tasksMap.get(parent)));
        	}
        	if (task.parents.isEmpty()) node.setEligible(true); 
        }
	}
	
	
}
