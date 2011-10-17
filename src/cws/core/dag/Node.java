package cws.core.dag;

import java.util.LinkedList;
import java.util.List;

public class Node {

	private int id;
	
	private List<Node> parents;
	private List<Node> children;
	
	private boolean done = false;
	private boolean eligible = false;
	
	public Node() {
		parents = new LinkedList<Node>();
		children = new LinkedList<Node>();
		
	}
	
	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public boolean isDone() {
		return done;
	}

	public void setDone(boolean done) {
		this.done = done;
	}

	public boolean isEligible() {
		return eligible;
	}

	public void setEligible(boolean eligible) {
		this.eligible = eligible;
	}
	
	public void addParent (Node parent) {
		parents.add(parent);
		parent.addChild(this);
	}
	
	private void addChild(Node child) {
		children.add(child);
	}
	
	public void updateEligibility() {
		if (isEligible()) return;
		setEligible(true);
		for (Node node : parents) {
			setEligible(isEligible() & node.isDone());
		}
	}
	
	public void updateChildren() {
		for (Node node : children) {
			node.updateEligibility();
		}
	}
	
	public void print(String indent) {
		System.out.println(indent + "L " + id);
		for (Node node : children) {
			node.print(indent+ "   ");
		}
	}
	
	public String printDot() {
	
		StringBuffer s = new StringBuffer();
		s.append("digraph G {");
		printDot(s);
		s.append("}");
		return s.toString();
	
	}
	
	private void printDot(StringBuffer s) {
		for (Node node : children) {
			s.append("\t" + getId() + " -> " + node.getId() + ";");
			node.printDot(s);
		}
	}
	
	
}
