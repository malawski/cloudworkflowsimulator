package cws.core.dag.algorithms;

import cws.core.dag.DAG;


/** 
 * 
 * This class computes simple parameters of the workflow, 
 * such as total runtime of all tasks.
 * 
 * @author malawski
 *
 */

public class Characteristics {
	
	private DAG dag;

	public Characteristics(DAG dag) {
		this.dag = dag;
	}
	
	/** 
	 * Computes sum of task runtimes, i.e. makespan on 1 processor.
	 * @return
	 */
	
	public double sumRuntime() {
		double sum = 0.0;		
		for (String taskName : dag.getTasks()) {
			sum+=dag.getTask(taskName).size;
		}		
		return sum;
	}

}
