package cws.core.dag;

import java.util.HashMap;
import java.util.List;

/**
 * 
 * @author Gideon Juve <juve@usc.edu>
 */
public class DAG {
    private HashMap<String, Double> files = new HashMap<String, Double>();
    private HashMap<String, Task> tasks = new HashMap<String, Task>();

    public DAG() {
    }

    public void addTask(Task t) {
        if (tasks.containsKey(t.id)) {
            throw new RuntimeException("Task already exists: " + t.id);
        }
        tasks.put(t.id, t);
    }

    public void addFile(String name, double size) {
        if (size < 0) {
            throw new RuntimeException("Invalid size for file '" + name + "': " + size);
        }
        files.put(name, size);
    }

    public void addEdge(String parent, String child) {
        Task p = tasks.get(parent);
        if (p == null) {
            throw new RuntimeException("Invalid edge: Parent not found: " + parent);
        }
        Task c = tasks.get(child);
        if (c == null) {
            throw new RuntimeException("Invalid edge: Child not found: " + child);
        }
        p.children.add(c);
        c.parents.add(p);
    }

    public void setInputs(String task, List<String> inputs) {
        Task t = getTask(task);
        t.inputs = inputs;
    }

    public void setOutputs(String task, List<String> outputs) {
        Task t = getTask(task);
        t.outputs = outputs;
    }

    public int numTasks() {
        return tasks.size();
    }

    public int numFiles() {
        return files.size();
    }

    public Task getTask(String id) {
        if (!tasks.containsKey(id)) {
            throw new RuntimeException("Task not found: " + id);
        }
        return tasks.get(id);
    }

    public double getFileSize(String name) {
        if (!files.containsKey(name)) {
            throw new RuntimeException("File not found: " + name);
        }
        return files.get(name);
    }

    public String[] getFiles() {
        return files.keySet().toArray(new String[0]);
    }

    public String[] getTasks() {
        return tasks.keySet().toArray(new String[0]);
    }
}
