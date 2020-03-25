package org.cloudbus.cloudsim.examples;

import java.text.DecimalFormat;
import java.util.*;
import java.util.Map.Entry;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.CloudletSchedulerTimeShared;
import org.cloudbus.cloudsim.Datacenter;
import org.cloudbus.cloudsim.DatacenterBroker;
import org.cloudbus.cloudsim.DatacenterCharacteristics;
import org.cloudbus.cloudsim.Host;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Pe;
import org.cloudbus.cloudsim.Storage;
import org.cloudbus.cloudsim.UtilizationModel;
import org.cloudbus.cloudsim.UtilizationModelFull;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.VmAllocationPolicySimple;
import org.cloudbus.cloudsim.VmSchedulerTimeShared;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.provisioners.BwProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.PeProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.RamProvisionerSimple;


public class cloudsimexample {
	
	private static List<Cloudlet> cloudletList;
	
	private static List<Vm> vmList;
	
	public static void main(String[] args) {

		Log.printLine("Starting CloudSimExample3...");

		try {
			// First step: Initialize the CloudSim package. It should be called
			// before creating any entities.
			int num_user = 1;   // number of cloud users
			Calendar calendar = Calendar.getInstance();
			boolean trace_flag = false;  // mean trace events

			// Initialize the CloudSim library
			CloudSim.init(num_user, calendar, trace_flag);

			// Second step: Create Datacenters
			//Datacenters are the resource providers in CloudSim. We need at list one of them to run a CloudSim simulation
			Datacenter datacenter0 = createDatacenter("Datacenter_0");

			//Third step: Create Broker
			DatacenterBroker broker = createBroker();
			int brokerId = broker.getId();

			//Fourth step: Create one virtual machine
			vmList = new ArrayList<Vm>();

			//VM description
			int vmid = 0;
			int mips = 250;
			long size = 10000; //image size (MB)
			int ram = 2048; //vm memory (MB)
			long bw = 1000;
			int pesNumber = 1; //number of cpus
			String vmm = "Xen"; //VMM name

			//create two VMs
			Vm vm1 = new Vm(vmid, brokerId, mips, pesNumber, ram, bw, size, vmm, new CloudletSchedulerTimeShared());

			//the second VM will have twice the priority of VM1 and so will receive twice CPU time
			vmid++;
			Vm vm2 = new Vm(vmid, brokerId, mips * 2, pesNumber, ram, bw, size, vmm, new CloudletSchedulerTimeShared());

			//add the VMs to the vmList
			vmList.add(vm1);
			vmList.add(vm2);

			//submit vm list to the broker
			broker.submitVmList(vmList);


			//Fifth step: Create two Cloudlets
			cloudletList = new ArrayList<Cloudlet>();

			//Cloudlet properties
			int id = 0;
			long length = 40000;
			long fileSize = 300;
			long outputSize = 300;
			UtilizationModel utilizationModel = new UtilizationModelFull();

			Cloudlet cloudlet1 = new Cloudlet(id, length, pesNumber, fileSize, outputSize, utilizationModel, utilizationModel, utilizationModel);
			cloudlet1.setUserId(brokerId);

			id++;
			Cloudlet cloudlet2 = new Cloudlet(id, length, pesNumber, fileSize, outputSize, utilizationModel, utilizationModel, utilizationModel);
			cloudlet2.setUserId(brokerId);

			//add the cloudlets to the list
			cloudletList.add(cloudlet1);
			cloudletList.add(cloudlet2);

			//submit cloudlet list to the broker
			broker.submitCloudletList(cloudletList);


			//bind the cloudlets to the vms. This way, the broker
			// will submit the bound cloudlets only to the specific VM
			broker.bindCloudletToVm(cloudlet1.getCloudletId(),vm1.getId());
			broker.bindCloudletToVm(cloudlet2.getCloudletId(),vm2.getId());

			// Sixth step: Starts the simulation
			CloudSim.startSimulation();


			// Final step: Print results when simulation is over
			List<Cloudlet> newList = broker.getCloudletReceivedList();
			bindCloudletToVmsMaxMin();
			CloudSim.stopSimulation();

        	printCloudletList(newList);

			//Print the debt of each user to each datacenter
			datacenter0.printDebts();

			Log.printLine("CloudSim Max-Min Algorithm finished!");
		}
		catch (Exception e) {
			e.printStackTrace();
			Log.printLine("The simulation has been terminated due to an unexpected error");
		}
	}

	private static Datacenter createDatacenter(String name){

		// Here are the steps needed to create a PowerDatacenter:
		// 1. We need to create a list to store
		//    our machine
		List<Host> hostList = new ArrayList<Host>();

		// 2. A Machine contains one or more PEs or CPUs/Cores.
		// In this example, it will have only one core.
		List<Pe> peList = new ArrayList<Pe>();

		int mips = 1000;

		// 3. Create PEs and add these into a list.
		peList.add(new Pe(0, new PeProvisionerSimple(mips))); // need to store Pe id and MIPS Rating

		//4. Create Hosts with its id and list of PEs and add them to the list of machines
		int hostId=0;
		int ram = 2048; //host memory (MB)
		long storage = 1000000; //host storage
		int bw = 10000;

		hostList.add(
    			new Host(
    				hostId,
    				new RamProvisionerSimple(ram),
    				new BwProvisionerSimple(bw),
    				storage,
    				peList,
    				new VmSchedulerTimeShared(peList)
    			)
    		); // This is our first machine

		//create another machine in the Data center
		List<Pe> peList2 = new ArrayList<Pe>();

		peList2.add(new Pe(0, new PeProvisionerSimple(mips)));

		hostId++;

		hostList.add(
    			new Host(
    				hostId,
    				new RamProvisionerSimple(ram),
    				new BwProvisionerSimple(bw),
    				storage,
    				peList2,
    				new VmSchedulerTimeShared(peList2)
    			)
    		); // This is our second machine



		// 5. Create a DatacenterCharacteristics object that stores the
		//    properties of a data center: architecture, OS, list of
		//    Machines, allocation policy: time- or space-shared, time zone
		//    and its price (G$/Pe time unit).
		String arch = "x86";      // system architecture
		String os = "Linux";          // operating system
		String vmm = "Xen";
		double time_zone = 10.0;         // time zone this resource located
		double cost = 3.0;              // the cost of using processing in this resource
		double costPerMem = 0.05;		// the cost of using memory in this resource
		double costPerStorage = 0.001;	// the cost of using storage in this resource
		double costPerBw = 0.0;			// the cost of using bw in this resource
		LinkedList<Storage> storageList = new LinkedList<Storage>();	//we are not adding SAN devices by now

        DatacenterCharacteristics characteristics = new DatacenterCharacteristics(
                arch, os, vmm, hostList, time_zone, cost, costPerMem, costPerStorage, costPerBw);

		// 6. Finally, we need to create a PowerDatacenter object.
		Datacenter datacenter = null;
		try {
			datacenter = new Datacenter(name, characteristics, new VmAllocationPolicySimple(hostList), storageList, 0);
		} catch (Exception e) {
			e.printStackTrace();
		}

		return datacenter;
	}

	//We strongly encourage users to develop their own broker policies, to submit vms and cloudlets according
	//to the specific rules of the simulated scenario
	private static DatacenterBroker createBroker(){

		DatacenterBroker broker = null;
		try {
			broker = new DatacenterBroker("Broker");
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
		return broker;
	}

	/**
	 * Prints the Cloudlet objects
	 * @param list  list of Cloudlets
	 */
	private static void printCloudletList(List<Cloudlet> list) {
		int size = list.size();
		Cloudlet cloudlet;

		String indent = "    ";
		Log.printLine();
		Log.printLine("========== OUTPUT ==========");
		Log.printLine("Cloudlet ID" + indent + "STATUS" + indent +
				"Data center ID" + indent + "VM ID" + indent + "Time" + indent + "Start Time" + indent + "Finish Time");

		DecimalFormat dft = new DecimalFormat("###.##");
		for (int i = 0; i < size; i++) {
			cloudlet = list.get(i);
			Log.print(indent + cloudlet.getCloudletId() + indent + indent);

			if (cloudlet.getCloudletStatus() == Cloudlet.SUCCESS){
				Log.print("SUCCESS");

				Log.printLine( indent + indent + cloudlet.getResourceId() + indent + indent + indent + cloudlet.getVmId() +
						indent + indent + dft.format(cloudlet.getActualCPUTime()) + indent + indent + dft.format(cloudlet.getExecStartTime())+
						indent + indent + dft.format(cloudlet.getFinishTime()));
			}
		}

	}
/* Max-Min algorithm */
	public static void bindCloudletToVmsMaxMin() 
	{
		int cloudletNum = cloudletList.size();
		int vmNum = vmList.size();
	
		// ready time for each machine; initially to be 0
		Double[] readyTime = new Double[vmNum];
		for (int i = 0; i < readyTime.length; i++) {
			readyTime[i] = 0.0;
		}
	
		// initialize the 2-dimensional matrix with ready time + completion time
		List<List<Double>> tasksVmsMatrix = create2DMatrix(cloudletList, vmList);
	
		int count = 1;
	
		do {
			System.out.println("===========================");
			System.out.println("This is start of iteration " + count);
			print2DArrayList(tasksVmsMatrix);
			// step 1: find smallest in each row; and find the largest of all;
			Map<Integer[], Double> map = findMaxMinTimeMap(tasksVmsMatrix);
			printMapForMaxMin(map);
	
			// step 2: retrieve all the info from the map
			Integer[] rowAndColIndexAndCloudletId = getRowAndColIndexesAndCloudletId(map);
			Double maxMin = getMinimumTimeValue(map);
			int rowIndex = rowAndColIndexAndCloudletId[0];
			int columnIndex = rowAndColIndexAndCloudletId[1];
			int cloudletId = rowAndColIndexAndCloudletId[2];
	
			// step 3: assign the cloudlet to the vm
			cloudletList.get(cloudletId).setVmId(vmList.get(columnIndex).getId());
			System.out.printf("The cloudlet %d has been assigned to VM %d \n", cloudletId, columnIndex);
	
			// step 4: update ready time array;
			Double oldReadyTime = readyTime[columnIndex];
			readyTime[columnIndex] = maxMin;
			System.out.printf("The ready time array is %s \n", Arrays.toString(readyTime));
	
			// step 5: update the cloudlet-vm matrix with current ready time
			updateTotalTimeMatrix(columnIndex, oldReadyTime, readyTime, tasksVmsMatrix);
	
			// step 6: remove the row after the cloudlet has been assigned
			tasksVmsMatrix.remove(rowIndex);
	
			System.out.println("This is end of iteration " + count);
			System.out.println("===========================");
			++count;
		} while (tasksVmsMatrix.size() > 0);
		calculateThroughputNew(readyTime, cloudletNum);
	}
	
	/* below are the helper functions for Max-Min */
	
	/*create a 2D matrix to represent task-vm relationship*/
	  /* row is cloudlet and column is vm, intersection cell is the expected*/
	private static List<List<Double>> create2DMatrix(List<? extends Cloudlet> cloudletList,List<? extends Vm> vmList)
	{
		List<List<Double>> table= new ArrayList<List<Double>>();
		for(int i=0;i<cloudletList.size();i++)
		{
			//original cloudlet id is added as last column
			Double originalCloudletId = (double) cloudletList.get(i).getCloudletId();
			//System.out.println("the original cloudlet id is:" + originalCloudletId);
			List<Double> temp=new ArrayList<Double>();
			for(int j=0;j<vmList.size();j++)
			{
				Double load=cloudletList.get(i).getCloudletLength()/vmList.get(j).getMips();
				temp.add(load);
			}
			temp.add(originalCloudletId);
			table.add(temp);
		}
		return table;
	}
	
	/* add initial ready time to completion time, in this specific case it makes*/
//	 private static void intializeTotalTime(List<List<Double>> taskVmsMatrix, Double[] readyTimeArray)
//	 {
//		 for(int i=0;i<taskVmsMatrix.size();i++)
//		 {
//			 List<Double> temp=new ArrayList<Double>();
//			 for(int j=0;j<readyTimeArray.length;j++)
//			 {
//				 Double readyTime=readyTimeArray[j];
//				 Double currentCompletionTime=taskVmsMatrix.get(i).get(j);
//				 currentCompletionTime+=readyTime;
//				 temp.add(currentCompletionTime);
//			 }
//			 taskVmsMatrix.set(i,temp);
//		 }
//	 }
	 
	/* iterate the matrix find max among min */
	private static Map<Integer[], Double> findMaxMinTimeMap(List<List<Double>> tasksVmsMatrix)
	{
		// step 1: scan each row and find the minimum
		int rowNum = tasksVmsMatrix.size();
		int colNum = tasksVmsMatrix.get(0).size();
		// last column is the cloudlet_id;
		int colNumWithoutLastColumn = colNum - 1;
	
		Map<Integer[], Double> map = new HashMap<Integer[], Double>();
	
		for (int row = 0; row < rowNum; row++) {
			// assume the first element in each row as minimum initially
			Double min = tasksVmsMatrix.get(row).get(0);
	
			// get the cloudlet_id
			Integer targetCloudletId = tasksVmsMatrix.get(row).get(colNumWithoutLastColumn).intValue();
	
			// info includes row, column and cloudlet_id
			// note the column number is initially 0
			Integer[] rowInfo = { row, 0, targetCloudletId };
	
			for (int col = 0; col < colNumWithoutLastColumn; col++) {
				Double current = tasksVmsMatrix.get(row).get(col);
				if (current < min) {
					min = current;
					rowInfo[1] = col; // get the column number
				}
			}
			map.put(rowInfo, min);
		}
	
		// step 2: find the max among the min_candidates in the map
		// it's a sorting problem basically
		// System.out.println("before sorting ");
		// printMapForMaxMin(map);
	
		// System.out.println("after sorting");
		HashMap<Integer[], Double> sortedMap = sortMapByValue(map);
		// printMapForMaxMin(sortedMap);
	
		Map<Integer[], Double> firstPair = getFirstPairFromMap(sortedMap);
		// printMapForMaxMin(firstPair);
		return firstPair;
	}
	
	/* sort the map by value */
	// http://www.java2novice.com/java-interview-programs/sort-a-map-by-value/
	// http://beginnersbook.com/2013/12/how-to-sort-hashmap-in-java-by-keys-and-values/
	// http://stackoverflow.com/questions/8119366/sorting-hashmap-by-values
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private static HashMap<Integer[], Double> sortMapByValue(Map<Integer[], Double> map)
	{
		Set<Entry<Integer[], Double>> set = map.entrySet();
		List<Entry<Integer[], Double>> list = new ArrayList<Entry<Integer[], Double>>(set);
	
		Collections.sort(list, new Comparator<Map.Entry<Integer[], Double>>() {
			public int compare(Map.Entry<Integer[], Double> o1, Map.Entry<Integer[], Double> o2) {
				return (o2.getValue()).compareTo(o1.getValue());
			}
		});
	
		// Here I am copying the sorted list in HashMap
		// using LinkedHashMap to preserve the insertion order
		HashMap sortedHashMap = new LinkedHashMap();
		for (Iterator it = list.iterator(); it.hasNext();) {
			Map.Entry entry = (Map.Entry) it.next();
			sortedHashMap.put(entry.getKey(), entry.getValue());
		}
		return sortedHashMap;
	}
	/*return the only one key from this map=>{row,column,cloudlet id} */
	private static Integer[] getRowAndColIndexesAndCloudletId(Map<Integer[],Double> map)
	{
		Integer[] key=new Integer[3];
		for(Entry<Integer[],Double> entry: map.entrySet())
		{
			key=entry.getKey();
		}
		return key;
	}
	/*return the only one value from this map =>minimum of expected completion */
	 private static Double getMinimumTimeValue(Map<Integer[],Double> map)
	 {
		 Double value=0.0;
		 for(Entry<Integer[],Double> entry: map.entrySet())
		 {
			 value=entry.getValue();
		 }
		 return value;
	 }
	 
	 /*update the expected completion time */
	 private static void updateTotalTimeMatrix(int columnIndex,Double oldReadyTime,Double[] readyTime,List<List<Double>> taskVmsMatrix)
	 {
		 //by adding current ready time to old corresponding expected completion time
		 Double newReadyTime =readyTime[columnIndex];
		 Double readyTimeDifference =newReadyTime- oldReadyTime;
		 for(int row=0;row<taskVmsMatrix.size();row++)
		 {
			 Double oldTotalTime=taskVmsMatrix.get(row).get(columnIndex);
			 Double newTotalTime=oldTotalTime+readyTimeDifference;
			 taskVmsMatrix.get(row).set(columnIndex,newTotalTime);
		 }
	 }
	 
	 private static void print2DArrayList(List<List<Double>> table)
	 {
		 System.out.printf("The current matirx is as below,with size of %d by %d\n", table.size(),table.get(0).size());
		 for(int i=0;i<table.size();i++)
		 {
			 for(int j=0;j<table.get(i).size();j++)
			 {
				 System.out.printf("%-11.5f", table.get(i).get(j));
			 }
			 System.out.printf("\n");
		 }
		 
	 }
	 
	private static void printMapForMaxMin(Map<Integer[], Double> map) {
		for (Entry<Integer[], Double> entry : map.entrySet()) {
			Integer[] key = entry.getKey();
			Double value = entry.getValue();
			System.out.printf("The keys are: {%d, %d, %d} ===> ", key[0], key[1], key[2]);
			System.out.printf("%.4f(%s), located at row %d column %d, and the cloudlet id is %d \n", value, "max",
					key[0], key[1], key[2]);
		}
	}
	
	/* get the first pair of map */
	// http://stackoverflow.com/questions/26230225/hashmap-getting-first-key-value
	private static Map<Integer[], Double> getFirstPairFromMap(Map<Integer[], Double> map) {
		Map.Entry<Integer[], Double> entry = map.entrySet().iterator().next();
		Integer[] key = entry.getKey();
		Double value = entry.getValue();
		Map<Integer[], Double> firstPair = new HashMap<Integer[], Double>();
		firstPair.put(key, value);
		return firstPair;
	}
	//private static void printMapForMaxMin(Map<Integer[],Double> map)
	//{
	//	for(Entry<Integer[],Double> entry : map.entrySet())
	//	{
	//		Integer[] key=entry.getKey();
	//		Double value=entry.getValue();
	//		System.out.println("The keys are:" + "{"+key[0]+","+key[1]+","+key[2]+"}");
	//		System.out.println("The min is"+value+", located at row "+key[0]+" column "+key[1]+" and the cloudlet id is "+key[2]);
	//	}
	//}
	
	public double calculateAvgTurnAroundTime(List<? extends Cloudlet> cloudletList)
	{
		double totalTime=0.0;
		int cloudletNum=cloudletList.size();
		for(int i=0;i<cloudletNum;i++)
		{
			totalTime+=cloudletList.get(i).getFinishTime();
			
		}
		double averageTurnAroundTime=totalTime/cloudletNum;
		System.out.printf("The average turnaround time is %.4f\n",averageTurnAroundTime);
		return averageTurnAroundTime;
	}
	/* calculate throughput */
	// http://stackoverflow.com/questions/1806816/java-finding-the-highest-value-in-an-array
	// we can actually get the result from the largest value in ready time
	// array,
	// in this case, this method can be used in DatacenterBroker.java,
	// the result will show up before the simulation though
	public double calculateThroughput(List<? extends Cloudlet> cloudletList)
	{
		double maxFinishTime=0.0;
		int cloudletNum=cloudletList.size();
		for(int i=0;i<cloudletNum;i++)
		{
			double currentFinishTime=cloudletList.get(i).getFinishTime();
			if(currentFinishTime>maxFinishTime)
				maxFinishTime=currentFinishTime;
		}
		double throughput=cloudletNum/maxFinishTime;
		System.out.printf("The throughput is %.6f\n",throughput);
		return throughput;
	}
	
	public static double calculateThroughputNew(Double[] readyTime, int cloudletNum) {
		List<Double> temp = new ArrayList<Double>(Arrays.asList(readyTime));
		Double throughput = cloudletNum / (Collections.max(temp) + 0.1);
		System.out.printf("The throughput is %.6f \n", throughput);
		return throughput;
	}
}