package cws.scenarios;

import java.util.ArrayList;

import org.cloudbus.cloudsim.CloudletSchedulerSpaceShared;
import org.cloudbus.cloudsim.Vm;

public class VmListGenerator {

	public static ArrayList<Vm> generateVmList(int number, int brokerId) {

		ArrayList<Vm> vmlist = new ArrayList<Vm>();
		
		int mips = 1000;
		long size = 10000; // image size (MB)
		int ram = 512; // vm memory (MB)
		long bw = 1000;
		int pesNumber = 1; // number of cpus
		String vmm = "Xen"; // VMM name
		
		// VM description
		for (int vmid = 0; vmid < number; vmid++) {

			// create VM
			Vm vm = new Vm(vmid, brokerId, mips, pesNumber, ram, bw, size, vmm, new CloudletSchedulerSpaceShared());

			// add the VM to the vmList
			vmlist.add(vm);
		}
		return vmlist;

	}

}
