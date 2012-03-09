package cws.core.experiment;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Random;

import org.cloudbus.cloudsim.distributions.ParetoDistr;

 /**
  * Class for generating list of DAG files for experiments.
  * @author malawski
  *
  */

public class DAGListGenerator {
	/** Possible DAG sizes */
    private static final int[] SIZES = new int[]{50, 100, 200, 300, 400, 500, 600, 700, 800, 900, 1000};
    
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

	
	public static String[] generateDAGListConstant(String name, int size, int length) {
		String dags[] = new String[length];
		for (int i=0; i<length; i++) {
				dags[i] = name + ".n." + size + "." + i%20 + ".dag";
		}
		return dags;
	}
	
	private static int[] generateUniformSizesArray(Random seed, int length) {
	    int[] sizes = new int[length];
        
        for (int i=0; i<length; i++) {
            int size = SIZES[seed.nextInt(SIZES.length)];
            sizes[i] = size;
        }
        
        return sizes;
    }
	
    private static ArrayList<String> generateDAGList(Random seed, String name, int[] sizes) {
        ArrayList<String> dags = new ArrayList<String>(sizes.length);
        
        for (int i=0; i<sizes.length; i++) {
            int size = sizes[i];
            int index = seed.nextInt(20);
            String dag = name + ".n." + size + "." + index + ".dag";
            dags.add(dag);
        }
        
        return dags;
    }
    
	public static String[] generateDAGListUniformUnsorted(Random seed, String name, int length) {
	    int[] sizes = generateUniformSizesArray(seed, length);
	    
	    ArrayList<String> dags = generateDAGList(seed, name, sizes);
	    
	    return dags.toArray(new String[0]);
	}
	
	public static String[] generateDAGListUniform(Random seed, String name, int length) {
	    int[] sizes = generateUniformSizesArray(seed, length);
        
        Arrays.sort(sizes);
        
        ArrayList<String> dags = generateDAGList(seed, name, sizes);
        
        Collections.reverse(dags);
        
        return dags.toArray(new String[0]);
	}
	
	public static String[] generateDAGListPareto(Random seed, String name, int length) {
		ArrayList<String> dags = new ArrayList<String>(length);
		
		ParetoDistr pareto = new ParetoDistr(seed, 1, 50);
		HashMap<Integer, Integer> distr = new HashMap<Integer, Integer>();
		for (int i = 0; i < length; i++) {
			double d = pareto.sample();
			int n;
			if (d < 100) {
				n = 50;
			} else if (d > 1000) {
				n = 1000;
			} else {
				n = (int) Math.floor(d / 100) * 100;
			}
			if (!distr.containsKey(n)) {
				distr.put(n, 1);
			} else {
				distr.put(n, distr.get(n) + 1);
			}
		}
		Integer[] sizes = distr.keySet().toArray(new Integer[0]);
		Arrays.sort(sizes);
		for (int size : sizes) {
			int count = distr.get(size);
			for (int i = 0; i < count; i++) {
				String dag = name + ".n." + size + "." + i % 20 + ".dag";
				dags.add(dag);
			}
		}
		Collections.reverse(dags);
		return dags.toArray(new String[0]);
	}
	
	public static String[] generateDAGListParetoUnsorted(Random seed, String name, int length) {
	    String[] list = generateDAGListPareto(seed, name, length);
	    
	    int n = list.length;
	    
	    // Use Fisher-Yates algorithm to randomize list
	    // To shuffle an array a of n elements (indices 0..n-1):
	    // for i from n − 1 downto 1 do
	    for (int i = n-1; i>=1; i--) {
	        // j = random integer with 0 ≤ j ≤ i
	        int j = seed.nextInt(i+1);
	        
	        //exchange a[j] and a[i];
	        String tmp = list[j];
	        list[j] = list[i];
	        list[i] = tmp;
	    }
	    
	    return list;
	}
}
