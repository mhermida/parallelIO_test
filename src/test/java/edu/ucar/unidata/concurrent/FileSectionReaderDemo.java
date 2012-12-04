package edu.ucar.unidata.concurrent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.Before;
import org.junit.Test;

import ucar.ma2.Array;
import ucar.ma2.InvalidRangeException;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;

public class FileSectionReaderDemo {

	//String location ="/share/testdata/NARR/narr-TMP-200mb_221_yyyymmdd_hh00_000.grb.grb2.nc4";
	String location ="/home/mhermida/data/nomads/narr-physaggs/narr-TMP-200mb_221_yyyymmdd_hh00_000.grb.grb2.nc4";
	String varName = "TMP_200mb";
	ExecutorService taskExecutor = null;
	private Variable var;
	
	@Before
	public void setUp() throws IOException{
		
		NetcdfFile file = NetcdfFile.open(location); 
		var = file.findVariable(varName);
		
	}	
	
	/*
	 *
	 * Base benchmark test for reading 100k times for one single point 
	 * from one netcdf4 file. 
	 *
	 */
	@Test
	public void baseBenchmark() throws IOException, InvalidRangeException{
		
		/*
		 * Read all times for a single point using var 
		 */		
		long startTime = System.currentTimeMillis();
									
		int[] shape = var.getShape();
		System.out.println( Arrays.toString(shape) );

		System.out.println("Starts reading var: "+var.getShortName() );		
		startTime = System.currentTimeMillis();
		int[] origin = new int[]{0,0,0};
		int[] wantedShape  = new int[]{ shape[0],1,1};		
		Array dataf =var.read(origin, wantedShape);
		long last = System.currentTimeMillis() - startTime;	
		System.out.println(dataf.getSizeBytes()+ " read in: "+ last +" milliseconds");
	
	}	
	
	
	/*
	 * Splits the reading task in 2*nProc(+1) subtasks 
	 */
	@Test
	public void fileSectionReaderAndExecutorServiceTest() throws InterruptedException, ExecutionException, IOException, InvalidRangeException{
		
						
		int nProc = Runtime.getRuntime().availableProcessors();
		taskExecutor = Executors.newFixedThreadPool(nProc);

		//List<Future<Array>> list = new ArrayList<Future<Array>>();
		Map<Integer,Future<Array>> resultsMap = new HashMap<>(); 
		int[] shape = var.getShape();
		System.out.println( Arrays.toString(shape) );
		
		int totalTimes = shape[0];		
		int timesPerOperation = totalTimes/(nProc*2); 
		int currentTime =0;
		int task = 0;			
		long start = System.currentTimeMillis();
		while( currentTime < totalTimes  ){

			if(currentTime+timesPerOperation > totalTimes ) 
				timesPerOperation = totalTimes - currentTime;
			
			int[] origin  = new int[]{currentTime,0,0};
			int[] section = new int[]{timesPerOperation, 1, 1};
			
			Callable<Array> worker = FileSectionReader.factory(location, varName, origin, section);
			Future<Array> submit = taskExecutor.submit(worker);
			//list.add(submit);
			resultsMap.put(task, submit);
			currentTime+=timesPerOperation;
			task++;
		}
		
		System.out.println("All tasks submitted: "+task);
		
		long bRead =0;
		//Put all data together 
		int[] wantedShape  = new int[]{ totalTimes,1,1};
		int dstPos=0;
		Array allData = Array.factory(var.getDataType(), wantedShape);
		for(int i=0; i< task; i++  ){
			Future<Array> future = resultsMap.get(i);
			Array d = future.get();
			bRead+=d.getSizeBytes();
			long tmpSize = d.getSize();
			Array.arraycopy(d, 0, allData, dstPos, (int)d.getSize());
			dstPos+=tmpSize;
		}
		
		taskExecutor.shutdown();
	
		System.out.println("All done. "+bRead+" bytes read in..."+ (System.currentTimeMillis() - start )+" ms.");

	} 
	
}
