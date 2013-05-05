package cws.core.dag;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;

import java.util.ArrayList;
import java.util.regex.Pattern;

import javax.xml.stream.Location;
import javax.xml.stream.StreamFilter;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import org.apache.commons.io.IOUtils;

/**
 * This class parses simulation DAGs from files in various formats.
 * 
 * @author Gideon Juve <juve@usc.edu>
 */
public class DAGParser {
    private DAGParser() {
    }

    /**
     * Parse a DAG from a file using the simple text DAG format. The format
     * consists of 5 different record types:
     * 
     * FILE filename size
     * - This record defines a file and its size in bytes.
     * 
     * TASK id type size
     * - This record defines a task, its type (or transformation) and its size
     * in seconds or MI (millions of instructions).
     * 
     * EDGE parent_id child_id
     * - This record defines a dependency between two tasks
     * 
     * INPUTS task_id filename...
     * - This record defines the inputs of a task
     * 
     * OUTPUTS task_id filename...
     * - This record defines the outputs of a task
     */
    public static DAG parseDAG(File dagfile) {
        DAG dag = new DAG();
        Pattern split = Pattern.compile("\\s+");

        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader(dagfile));
            for (String line = br.readLine(); line != null; line = br.readLine()) {
                line = line.trim();

                // Ignore comments
                if (line.startsWith("#")) {
                    continue;
                }

                // Ignore empty lines
                if (line.length() == 0) {
                    continue;
                }

                // Split record
                String[] rec = split.split(line);
                String type = rec[0];

                if ("TASK".equalsIgnoreCase(type)) {
                    if (rec.length != 4) {
                        throw new RuntimeException("Invalid TASK record: " + line);
                    }
                    String id = rec[1];
                    String xform = rec[2];
                    double size = Double.parseDouble(rec[3]);
                    dag.addTask(new Task(id, xform, size));
                } else if ("FILE".equalsIgnoreCase(type)) {
                    if (rec.length != 3) {
                        throw new RuntimeException("Invalid FILE record: " + line);
                    }
                    String name = rec[1];
                    double size = Double.parseDouble(rec[2]);
                    dag.addFile(name, size);
                } else if ("EDGE".equalsIgnoreCase(type)) {
                    if (rec.length != 3) {
                        throw new RuntimeException("Invalid EDGE record: " + line);
                    }
                    String parent = rec[1];
                    String child = rec[2];
                    dag.addEdge(parent, child);
                } else if ("INPUTS".equalsIgnoreCase(type)) {
                    if (rec.length < 3) {
                        throw new RuntimeException("Invalid INPUTS record: " + line);
                    }
                    ArrayList<DAGFile> inputs = new ArrayList<DAGFile>(rec.length - 2);
                    String task = rec[1];
                    for (int i = 2; i < rec.length; i++) {
                        DAGFile file = new DAGFile(rec[i], dag.getFileSize(rec[i]));
                        inputs.add(file);
                    }
                    dag.setInputs(task, inputs);
                } else if ("OUTPUTS".equalsIgnoreCase(type)) {
                    if (rec.length < 3) {
                        throw new RuntimeException("Invalid OUTPUTS record: " + line);
                    }
                    ArrayList<DAGFile> outputs = new ArrayList<DAGFile>(rec.length - 2);
                    String task = rec[1];
                    for (int i = 2; i < rec.length; i++) {
                        DAGFile file = new DAGFile(rec[i], dag.getFileSize(rec[i]));
                        outputs.add(file);
                    }
                    dag.setOutputs(task, outputs);
                } else {
                    throw new RuntimeException("Unable to read DAG: invalid record: " + line);
                }
            }
        } catch (IOException ioe) {
            throw new RuntimeException("Unable to read DAG: I/O error", ioe);
        } finally {
            IOUtils.closeQuietly(br);
        }
        return dag;
    }

    /**
     * Parse a DAG from the DAX-like synthetic workflows available here:
     * https://confluence.pegasus.isi.edu/display/pegasus/WorkflowGenerator
     * 
     * This format does not conform to the actual DAX schema used by Pegasus
     * even though the files may refer to the Pegasus DAX 2.1 schema.
     * 
     * The format these files follow is:
     * <adag ...>
     * <job id= name= namespace= version= runtime=>
     * <uses file= link= size= .../>
     * [<uses/>...]
     * </job>
     * [<job/>...]
     * <child ref=>
     * <parent ref=/>
     * [<parent/>...]
     * </child>
     * [<child/>...]
     * </adag>
     * 
     * Other attributes are ignored. Other elements are errors.
     * 
     * <job> = task
     * The namespace::name:version of a <job> is its transformation.
     * The link attr of <uses> specifies if the file is an "input" or "output"
     * The ref attr of <parent> and <child> specifies the job ID
     */
    public static DAG parseDAX(File daxfile) {
        DAG dag = new DAG();
        FileInputStream fis = null;
        XMLStreamReader xmlReader = null;
        try {
            /*
             * This filters an XML parsing stream to eliminate everything
             * except the START_ELEMENT and END_ELEMENT events.
             */
            StreamFilter filter = new StreamFilter() {
                @Override
                public boolean accept(XMLStreamReader reader) {
                    return reader.isStartElement() || reader.isEndElement();
                }
            };

            // Set up StAX parser
            XMLInputFactory f = XMLInputFactory.newInstance();
            fis = new FileInputStream(daxfile);
            xmlReader = f.createFilteredReader(f.createXMLStreamReader(fis, "UTF-8"), filter);

            // Sanity check
            if (!"adag".equals(xmlReader.getLocalName())) {
                Location l = xmlReader.getLocation();
                throw new RuntimeException(String.format("Unexpected element '%s' at %d:%d", xmlReader.getLocalName(),
                        l.getLineNumber(), l.getColumnNumber()));
            }

            xmlReader.next(); // Skip over <adag>

            // While we have not seen </adag>
            while (!xmlReader.isEndElement()) {
                // <job> element
                if ("job".equalsIgnoreCase(xmlReader.getLocalName())) {

                    // Parse task
                    String id = xmlReader.getAttributeValue(null, "id");
                    String ns = xmlReader.getAttributeValue(null, "namespace");
                    String name = xmlReader.getAttributeValue(null, "name");
                    String version = xmlReader.getAttributeValue(null, "version");
                    double runtime = Double.parseDouble(xmlReader.getAttributeValue(null, "runtime"));

                    // Transformation name is namespace::name:version
                    String transformation = String.format("%s::%s:%s", ns, name, version);

                    // Add the task to the dag
                    dag.addTask(new Task(id, transformation, runtime));

                    xmlReader.next(); // to first <uses> or </job>

                    // List of input files and output files for the task
                    ArrayList<DAGFile> inputs = new ArrayList<DAGFile>();
                    ArrayList<DAGFile> outputs = new ArrayList<DAGFile>();

                    while (!xmlReader.isEndElement()) {
                        // Sanity check
                        if (!"uses".equals(xmlReader.getLocalName())) {
                            Location l = xmlReader.getLocation();
                            throw new RuntimeException(String.format("Unexpected element '%s' at %d:%d",
                                    xmlReader.getLocalName(), l.getLineNumber(), l.getColumnNumber()));
                        }

                        // Parse file info
                        String fileName = xmlReader.getAttributeValue(null, "file");
                        long size = Long.parseLong(xmlReader.getAttributeValue(null, "size"));
                        String link = xmlReader.getAttributeValue(null, "link");

                        // Add the file to the dag
                        dag.addFile(fileName, size);
                        DAGFile file = new DAGFile(fileName, size);

                        // Determine if the file is an input or an output
                        if ("input".equalsIgnoreCase(link)) {
                            inputs.add(file);
                        } else if ("output".equalsIgnoreCase(link)) {
                            outputs.add(file);
                        } else {
                            throw new RuntimeException(String.format("Invalid link '%s' for file '%s'", link, fileName));
                        }

                        xmlReader.next(); // to </uses>
                        xmlReader.next(); // to next <uses> or </job>
                    }

                    // Set input output files for the job
                    dag.setInputs(id, inputs);
                    dag.setOutputs(id, outputs);
                }

                // <child> element
                else if ("child".equalsIgnoreCase(xmlReader.getLocalName())) {

                    String child = xmlReader.getAttributeValue(null, "ref");

                    xmlReader.next(); // to first <parent> or </child>

                    while (!xmlReader.isEndElement()) {
                        // Sanity check
                        if (!"parent".equals(xmlReader.getLocalName())) {
                            Location l = xmlReader.getLocation();
                            throw new RuntimeException(String.format("Unexpected element '%s' at %d:%d",
                                    xmlReader.getLocalName(), l.getLineNumber(), l.getColumnNumber()));
                        }

                        String parent = xmlReader.getAttributeValue(null, "ref");

                        // System.out.printf("%s -> %s\n", parent, child);
                        dag.addEdge(parent, child);

                        xmlReader.next(); // to </parent>
                        xmlReader.next(); // to next <parent> or </child>
                    }
                }

                // Unknown element
                else {
                    // Sanity check
                    Location l = xmlReader.getLocation();
                    throw new RuntimeException(String.format("Unknown element '%s' at %d:%d", xmlReader.getLocalName(),
                            l.getLineNumber(), l.getColumnNumber()));
                }

                xmlReader.next(); // From (</job>,</child>) to (<job>,<child>,</adag>)
            }

        } catch (IOException ioe) {
            throw new RuntimeException("Unable to parse DAX: I/O error", ioe);
        } catch (XMLStreamException xse) {
            throw new RuntimeException("Unable to parse DAX: XML parser error", xse);
        } finally {
            IOUtils.closeQuietly(fis);
            try {
                if (xmlReader != null)
                    xmlReader.close();
            } catch (XMLStreamException xse) {
                throw new RuntimeException("Unable to parse DAX: XML parser error", xse);
            }
        }
        return dag;
    }
}
