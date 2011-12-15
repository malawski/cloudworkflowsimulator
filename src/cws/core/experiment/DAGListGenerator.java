package cws.core.experiment;

 /**
  * Class for generating list of DAG files for experiments.
  * @author malawski
  *
  */

public class DAGListGenerator {
	
	
	/**
	 * Generates list of DAG names (.dag files) to be used with workflows 
	 * from the workflow generator 
	 * https://confluence.pegasus.isi.edu/display/pegasus/WorkflowGenerator
	 * converted to simplified dag format using dax2dag.rb script
	 * It is better to use DAG files, since XML parser tends to be 10x slower.
	 * 
	 * @param name Name prefix of DAG file (e.g. MONTAGE)
	 * @param sizes array of sizes, e.g. {100, 200}
	 * @param sizeCount number of files of each size (usually 20)
	 * @return array of file names (no path)
	 */
	
	public static String[] generateDAGList(String name, int [] sizes, int sizeCount ) {
		
		String dags[] = new String[sizes.length*sizeCount];
		for (int i=0; i<sizes.length; i++) {
			for (int j=0; j< sizeCount; j++) {
				dags[i*sizeCount + j] = name + ".n." + sizes[i] + "." + j + ".dag";
			}
		}
		return dags;
	}
}
