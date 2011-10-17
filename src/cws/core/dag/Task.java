package cws.core.dag;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author Gideon Juve <juve@usc.edu>
 */
public class Task {
    public String id = null;
    public String transformation = null;
    public double size = 0.0;
    public List<Task> parents = new ArrayList<Task>(2);
    public List<Task> children = new ArrayList<Task>(5);
    public List<String> inputs = null;
    public List<String> outputs = null;
    
    public Task(String id, String transformation, double size) {
        this.id = id;
        this.transformation = transformation;
        this.size = size;
    }
    
    @Override
    public int hashCode() {
        return id.hashCode();
    }
    
    @Override
    public boolean equals(Object o) {
        if (!(o instanceof Task)) {
            return false;
        }
        Task t = (Task)o;
        return this.id.equals(t.id);
    }
    
    @Override
    public String toString() {
        return id;
    }
}
