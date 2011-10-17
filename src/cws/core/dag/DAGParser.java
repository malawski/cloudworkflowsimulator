package cws.core.dag;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import java.util.ArrayList;
import java.util.regex.Pattern;

/**
 *
 * @author Gideon Juve <juve@usc.edu>
 */
public class DAGParser {
    private DAGParser() {}
    
    public static DAG parseDAG(File dagfile) {
        DAG dag = new DAG();
        Pattern split = Pattern.compile("\\s+");
        
        
        try {
            BufferedReader br = new BufferedReader(new FileReader(dagfile));
            for (String line = br.readLine(); line != null; line = br.readLine()) {
                line = line.trim();
                
                // Ignore comments
                if (line.startsWith("#")) {
                    continue;
                }
                
                // Ignore empty lines
                if (line.length()==0) {
                    continue;
                }
                
                // Split record
                String[] rec = split.split(line);
                String type = rec[0];
                
                if ("TASK".equalsIgnoreCase(type)) {
                    if (rec.length != 4) {
                        throw new RuntimeException("Invalid TASK record: "+line);
                    }
                    String id = rec[1];
                    String xform = rec[2];
                    double size = Double.parseDouble(rec[3]);
                    dag.addTask(new Task(id, xform, size));
                }
                else if ("FILE".equalsIgnoreCase(type)) {
                    if (rec.length != 3) {
                        throw new RuntimeException("Invalid FILE record: "+line);
                    }
                    String name = rec[1];
                    double size = Double.parseDouble(rec[2]);
                    dag.addFile(name,  size);
                }
                else if ("EDGE".equalsIgnoreCase(type)) {
                    if (rec.length != 3) {
                        throw new RuntimeException("Invalid EDGE record: "+line);
                    }
                    String parent = rec[1];
                    String child = rec[2];
                    dag.addEdge(parent, child);
                }
                else if ("INPUTS".equalsIgnoreCase(type)) {
                    if (rec.length < 3) {
                        throw new RuntimeException("Invalid INPUTS record: "+line);
                    }
                    ArrayList<String> inputs = new ArrayList<String>(rec.length-2);
                    String task = rec[1];
                    for (int i=2; i<rec.length; i++) {
                        inputs.add(rec[i]);
                    }
                    dag.setInputs(task, inputs);
                }
                else if ("OUTPUTS".equalsIgnoreCase(type)) {
                    if (rec.length < 3) {
                        throw new RuntimeException("Invalid OUTPUTS record: "+line);
                    }
                    ArrayList<String> outputs = new ArrayList<String>(rec.length-2);
                    String task = rec[1];
                    for (int i=2; i<rec.length; i++) {
                        outputs.add(rec[i]);
                    }
                    dag.setOutputs(task, outputs);
                }
                else {
                    throw new RuntimeException("Unable to read DAG: invalid record: "+line);
                }
            }
            br.close();
        } catch(IOException ioe) {
            throw new RuntimeException("Unable to read DAG: I/O error", ioe);
        }
        
        return dag;
    }
}
