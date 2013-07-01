package cws.core.dag;

import java.util.HashMap;
import java.util.List;

import cws.core.dag.exception.DAGFileNotFoundException;
import cws.core.storage.StorageManager;

/**
 * 
 * @author Gideon Juve <juve@usc.edu>
 */
public class DAG {
    private HashMap<String, Long> files = new HashMap<String, Long>();
    private HashMap<String, Task> tasks = new HashMap<String, Task>();

    public void addTask(Task t) {
        if (tasks.containsKey(t.getId())) {
            throw new RuntimeException("Task already exists: " + t.getId());
        }
        tasks.put(t.getId(), t);
    }

    public void addFile(String name, long size) {
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
        p.getChildren().add(c);
        c.getParents().add(p);
    }

    public double getRuntimeSum() {
        return getRuntimeSum(null);
    }

    public double getRuntimeSum(StorageManager storageManager) {
        double sum = 0.0;
        for (String taskName : getTasks()) {
            sum += getTaskById(taskName).getPredictedRuntime(storageManager);
        }
        return sum;
    }

    public void setInputs(String taskId, List<DAGFile> inputs) {
        Task t = getTaskById(taskId);
        t.addInputFiles(inputs);
    }

    public void setOutputs(String task, List<DAGFile> outputs) {
        Task t = getTaskById(task);
        t.addOutputFiles(outputs);
    }

    public int numTasks() {
        return tasks.size();
    }

    public int numFiles() {
        return files.size();
    }

    public Task getTaskById(String id) {
        if (!tasks.containsKey(id)) {
            throw new RuntimeException("Task not found: " + id);
        }
        return tasks.get(id);
    }

    public long getFileSize(String name) {
        if (!files.containsKey(name)) {
            throw new DAGFileNotFoundException(name);
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
