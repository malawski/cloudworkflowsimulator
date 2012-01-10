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
        if (size <= 0) {
            throw new RuntimeException(
                    "Invalid size for task "+id+" ("+transformation+"): "+size);
        }
    }
    
    /* IT IS IMPORTANT THAT THESE ARE NOT IMPLEMENTED
    
    Using the default implementation allows us to put Tasks from
    different DAGs that have the same task ID into a single HashMap
    or HashSet. That way we don't have to maintain a reference from
    the Task to the DAG that owns it--we can mix tasks from different
    DAGs in the same data structure.
    
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
    */
    
    @Override
    public String toString() {
        return id;
    }
}
