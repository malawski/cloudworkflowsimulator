package cws.scenarios;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.DecimalFormat;
import java.util.List;
import java.util.Map;


import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Vm;

import cws.core.dag.Job;

public class Helper {
	
	private static String OUTPUT_PATH = "output";
	
	/**
	 * Prints the Cloudlet objects.
	 *
	 * @param list list of Cloudlets
	 */
	public static void printCloudletList(List<Cloudlet> list, String fileName) {
		int size = list.size();
		Cloudlet cloudlet;

		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw, true);
		
		
		String indent = "    ";
		pw.println();
		pw.println("========== OUTPUT ==========");
		pw.println("Cloudlet ID" + indent + "STATUS" + indent
				+ "Data center ID" + indent + "VM ID" + indent + "Time" + indent
				+ "Start Time" + indent + "Finish Time");

		DecimalFormat dft = new DecimalFormat("###.##");
		for (int i = 0; i < size; i++) {
			cloudlet = list.get(i);
			pw.print(indent + cloudlet.getCloudletId() + indent + indent);

			if (cloudlet.getCloudletStatus() == Cloudlet.SUCCESS) {
				pw.print("SUCCESS");

				pw.println(indent + indent + cloudlet.getResourceId()
						+ indent + indent + indent + cloudlet.getVmId()
						+ indent + indent
						+ dft.format(cloudlet.getActualCPUTime()) + indent
						+ indent + dft.format(cloudlet.getExecStartTime())
						+ indent + indent
						+ dft.format(cloudlet.getFinishTime()));
			}
		}
		Log.print(sw.toString());
		stringToFile(sw.toString(),  fileName + ".txt");

	}
	
	public static void saveDot(Job job, String fileName) {
		
		stringToFile(job.getFanInNode().printDot(), fileName + ".dot");
	}
	
	public static void stringToFile(String s, String fileName) {
		BufferedWriter out;
		try {
			out = new BufferedWriter(new FileWriter(OUTPUT_PATH + File.separator + fileName));
			out.write(s);
			out.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
	}

	public static void printVmList(Map<Integer, Double> vmCreationTimes, Map<Integer, Double> vmDestroyTimes, String name) {

		Vm vm;

		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw, true);
		
		
		String indent = "    ";
		pw.println();
		pw.println("========== VMs ==========");
		pw.println("VM ID" + indent + "Creation Time" + indent
				+ "Destroy Time");

		DecimalFormat dft = new DecimalFormat("###.##");

		for (Integer vmid : vmCreationTimes.keySet() ) {
			
			pw.print(indent + vmid + indent + indent);

				pw.println(indent + indent + dft.format(vmCreationTimes.get(vmid))
						+ indent + indent
						+ dft.format(vmDestroyTimes.get(vmid))
						);
			}
		
		Log.print(sw.toString());
		stringToFile(sw.toString(), name + "-vms.txt");
		
	}

}
