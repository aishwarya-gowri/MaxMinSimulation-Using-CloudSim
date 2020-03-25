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
	
	

/* Max-Min algorithm */
	public void bindCloudletToVmsMaxMin() 
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
	 private static void intializeTotalTime(List<List<Double>> taskVmsMatrix, Double[] readyTimeArray)
	 {
		 for(int i=0;i<taskVmsMatrix.size();i++)
		 {
			 List<Double> temp=new ArrayList<Double>();
			 for(int j=0;j<readyTimeArray.length;j++)
			 {
				 Double readyTime=readyTimeArray[j];
				 Double currentCompletionTime=taskVmsMatrix.get(i).get(j);
				 currentCompletionTime+=readyTime;
				 temp.add(currentCompletionTime);
			 }
			 taskVmsMatrix.set(i,temp);
		 }
	 }
	 
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