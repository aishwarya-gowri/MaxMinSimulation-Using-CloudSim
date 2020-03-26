package org.cloudbus.cloudsim.examples;


import java.text.DecimalFormat;
import java.util.*;
import java.util.Map.Entry;

import java.util.Random;
import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.CloudletSchedulerTimeShared;
import org.cloudbus.cloudsim.CloudletSchedulerSpaceShared;
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
import org.cloudbus.cloudsim.VmSchedulerSpaceShared;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.provisioners.BwProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.PeProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.RamProvisionerSimple;


public class cloudsimexample {
	
	/*Global variables*/
	private static List<Cloudlet> cloudletList;
	
	private static List<Vm> vmList;
	
	/* function to create vmlist*/
	
	private static List<Vm> createVM(int userId, int vms, int idShift) {
		//Creates a container to store VMs. This list is passed to the broker later
		LinkedList<Vm> list = new LinkedList<Vm>();
		Random rand=new Random();
		//VM Parameters
		long size = 10000; //image size (MB)
		int ram = 512; //vm memory (MB)
		int mips;
		long bw = 1000;
		int pesNumber = 1; //number of cpus
		String vmm = "Xen"; //VMM name

		//create VMs
		Vm[] vm = new Vm[vms];

		for(int i=0;i<vms;i++){
			mips=rand.nextInt(250);
			vm[i] = new Vm(idShift + i, userId,mips , pesNumber, ram, bw, size, vmm, new CloudletSchedulerSpaceShared());
			list.add(vm[i]);
			System.out.println("");
			System.out.println("Vm"+i+"  mips:"+mips);
		}
		 
		return list;
	}

	/* function to create CloudletList */

	private static List<Cloudlet> createCloudlet(int userId, int cloudlets, int idShift){
		// Creates a container to store Cloudlets
		LinkedList<Cloudlet> list = new LinkedList<Cloudlet>();
		Random rand=new Random();
		//cloudlet parameters
		
		long fileSize = 300;
		long outputSize = 300;
		int pesNumber = 1;
		UtilizationModel utilizationModel = new UtilizationModelFull();

		Cloudlet[] cloudlet = new Cloudlet[cloudlets];

		for(int i=0;i<cloudlets;i++){
			cloudlet[i] = new Cloudlet(idShift + i,rand.nextInt(40000), pesNumber, fileSize, outputSize, utilizationModel, utilizationModel, utilizationModel);
			// setting the owner of these Cloudlets
			cloudlet[i].setUserId(userId);
			list.add(cloudlet[i]);
		}

		return list;
	}
	
	/**
	 * execution starts here
	 **/
	
	public static void main(String[] args) {
		Log.printLine("Starting CloudSimProject Max-Min...");

		try {
			//Initialize the CloudSim package. 
			int num_user = 2;   // number of grid users
			Calendar calendar = Calendar.getInstance();
			boolean trace_flag = false;  // mean trace events

			// Initialize the CloudSim library
			CloudSim.init(num_user, calendar, trace_flag);

			//  Create Datacenters
			//Datacenters are the resource providers in CloudSim. We need atleast one of them to run a CloudSim simulation
			Datacenter datacenter0 = createDatacenter("Datacenter_0");
			Datacenter datacenter1 = createDatacenter("Datacenter_1");
			
			//Create Broker 
			//Broker assigns cloudlets to vms and registers the vm in a datacenter
			DatacenterBroker broker = createBroker("Broker_0");
			int brokerId = broker.getId();

			//Create VMs and Cloudlets and send them to broker
			vmList = createVM(brokerId, 5, 0); //creating 5 vms
			cloudletList = createCloudlet(brokerId, 10, 0); // creating 10 cloudlets

			broker.submitVmList(vmList);
			broker.submitCloudletList(cloudletList);
			bindCloudletToVmsMaxMin();
			
			//Start the simulation
			CloudSim.startSimulation();

			//Print results when simulation is over
			
			List<Cloudlet> newList = broker.getCloudletReceivedList();
			
			//Stop the simulation			
			CloudSim.stopSimulation();

			printCloudletList(newList);

			Log.printLine("CloudSimProject Max-Min finished!");
		}
		catch (Exception e)
		{
			e.printStackTrace();
			Log.printLine("The simulation has been terminated due to an unexpected error");
		} 
	}

	private static Datacenter createDatacenter(String name){

		//create a list to store one or more Machines
		List<Host> hostList = new ArrayList<Host>();

		//A Machine contains one or more PEs(Processing elements) or CPUs/Cores. 
		//Therefore, should create a list to store these PEs before creating a Machine.
		List<Pe> peList1 = new ArrayList<Pe>();

		int mips = 1000;

		// Create PEs and add these into the list.
		//for a quad-core machine, a list of 4 PEs is required:
		peList1.add(new Pe(0, new PeProvisionerSimple(mips)));// need to store Pe id and MIPS Rating
		peList1.add(new Pe(1, new PeProvisionerSimple(mips)));
		peList1.add(new Pe(2, new PeProvisionerSimple(mips)));
		peList1.add(new Pe(3, new PeProvisionerSimple(mips)));

		//Another list, for a dual-core machine
		List<Pe> peList2 = new ArrayList<Pe>();

		peList2.add(new Pe(0, new PeProvisionerSimple(mips)));
		peList2.add(new Pe(1, new PeProvisionerSimple(mips)));

		//Create Hosts with its id and list of PEs and add them to the list of machines
		int hostId=0;
		int ram = 16384; //host memory (MB)
		long storage = 1000000; //host storage
		int bw = 10000;

		hostList.add(
    			new Host(
    				hostId,
    				new RamProvisionerSimple(ram),
    				new BwProvisionerSimple(bw),
    				storage,
    				peList1,
    				new VmSchedulerTimeShared(peList1)
    			)
    		); // first machine

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
    		); // Second machine

		// Create a DatacenterCharacteristics object that stores the properties of a data center
		String arch = "x86";      // system architecture
		String os = "Linux";          // operating system
		String vmm = "Xen";
		
		double time_zone = 10.0;         // time zone this resource located
		double cost = 3.0;              // the cost of using processing in this resource
		double costPerMem = 0.05;		// the cost of using memory in this resource
		double costPerStorage = 0.1;	// the cost of using storage in this resource
		double costPerBw = 0.1;			// the cost of using bw in this resource
		LinkedList<Storage> storageList = new LinkedList<Storage>();	

		DatacenterCharacteristics characteristics = new DatacenterCharacteristics(
                arch, os, vmm, hostList, time_zone, cost, costPerMem, costPerStorage, costPerBw);


		//create a PowerDatacenter object.
		Datacenter datacenter = null;
		try {
			datacenter = new Datacenter(name, characteristics, new VmAllocationPolicySimple(hostList), storageList, 0);
		} catch (Exception e) {
			e.printStackTrace();
		} 
		
		return datacenter;
	}

	private static DatacenterBroker createBroker(String name){

		DatacenterBroker broker = null;
		try {
			broker = new DatacenterBroker(name);
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
		String newline="\n";
		Log.printLine();
		Log.printLine("========== OUTPUT ==========");
		Log.printLine("Cloudlet ID" + indent+ indent + "STATUS" + indent + indent+
				"Data center ID" + indent + indent+ "VM ID" + indent + indent + "    "+"Time" + indent + indent+ indent+ indent +"Start Time" + indent + indent+ indent + indent+ "Finish Time");
	    String indent2="      ";
		DecimalFormat dft = new DecimalFormat("###.##");
		for (int i = 0; i < size; i++) {
			cloudlet = list.get(i);
			System.out.printf(indent + cloudlet.getCloudletId() + indent + indent);
			
			if (cloudlet.getCloudletStatus() == Cloudlet.SUCCESS){
				System.out.printf(indent2+"SUCCESS");
				
//				Log.printLine( indent + indent + indent+ cloudlet.getResourceId() + indent + indent + indent + indent+"   "+ cloudlet.getVmId() +
//						indent + indent + indent + dft.format(cloudlet.getActualCPUTime()) +
//						indent + indent + dft.format(cloudlet.getExecStartTime())+ indent + indent + indent + dft.format(cloudlet.getFinishTime()));
				System.out.printf( indent + indent + indent+ cloudlet.getResourceId() + indent + indent + indent + indent+"   "+ cloudlet.getVmId()+ indent + indent+ indent);
				System.out.printf("%-22.5f",cloudlet.getActualCPUTime());
				System.out.printf("%-25.5f", cloudlet.getExecStartTime());
				System.out.printf("%-22.5f", cloudlet.getFinishTime());
				System.out.println("");
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
	private static List<List<Double>> create2DMatrix(List<Cloudlet> cloudletList,List<Vm> vmList)
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
	
		
		// find the max among the min_candidates in the map
		// it's a sorting problem basically
//		System.out.println("before sorting ");
//		printMapForMaxMin(map);
	
		//System.out.println("after sorting");
		HashMap<Integer[], Double> sortedMap = sortMapByValue(map);
		//printMapForMaxMin(sortedMap);
	
		Map<Integer[], Double> firstPair = getFirstPairFromMap(sortedMap);
		// printMapForMaxMin(firstPair);
		return firstPair;
	}
	
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
	
		// copying the sorted list in HashMap
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
	 
	 /* print current cloudlet vs vm matrix */ 
	 
	 private static void print2DArrayList(List<List<Double>> table)
	 {
		 String indent="           ";
		 System.out.println("The current required exceution time matirx is as below,with size of "+ table.size()+" by "+table.get(0).size());
		 //System.out.printf(indent);
		 for(int j=0;j<5;j++)
		 {
			 System.out.printf("    Vm"+j+indent);
		 }
		 System.out.println("cloudletNum");
		 for(int i=0;i<table.size();i++)
		 {
	 		//System.out.printf(indent);
	 		 String indent2="   ";
			 for(int j=0;j<table.get(i).size();j++)
			 {
				 System.out.printf("%-15.5f", table.get(i).get(j));
				 System.out.printf(indent2);
			 }
			 System.out.printf("\n");
		 }
		 
	 }
	 
	 /* print selected cloudlet-vm pair */ 
	 
	private static void printMapForMaxMin(Map<Integer[], Double> map) {
		for (Entry<Integer[], Double> entry : map.entrySet()) {
			Integer[] key = entry.getKey();
			Double value = entry.getValue();
			System.out.println("");
			System.out.printf("The required values {row,column,cloudlet id} : {%d, %d, %d} ===> ", key[0], key[1], key[2]);
			System.out.printf("%.4f(%s), located at row %d column %d, and the cloudlet id is %d \n", value, "max",
					key[0], key[1], key[2]);
		}
	}
	
	/* get the first pair of map */
	
	private static Map<Integer[], Double> getFirstPairFromMap(Map<Integer[], Double> map) {
		Map.Entry<Integer[], Double> entry = map.entrySet().iterator().next();
		Integer[] key = entry.getKey();
		Double value = entry.getValue();
		Map<Integer[], Double> firstPair = new HashMap<Integer[], Double>();
		firstPair.put(key, value);
		return firstPair;
	}
	
	/* calculate average turn-around time */
	
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
	
	/* calculate average throughput */
	
	public static double calculateThroughputNew(Double[] readyTime, int cloudletNum) {
		List<Double> temp = new ArrayList<Double>(Arrays.asList(readyTime));
		Double throughput = cloudletNum / (Collections.max(temp) + 0.1);
		System.out.printf("The throughput is %.6f \n", throughput);
		return throughput;
	}
}