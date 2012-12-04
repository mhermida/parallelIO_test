package edu.ucar.unidata.concurrent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.Before;
import org.junit.Test;

import ucar.ma2.Array;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;
import ucar.nc2.dataset.NetcdfDataset;
import ucar.nc2.dt.grid.GridDataset;

public class VariableSectionReaderDemo {

	String location ="/home/mhermida/data/nomads/narr-physaggs/narr-TMP-200mb_221_yyyymmdd_hh00_000.grb.grb2.nc4";
	String varName = "TMP_200mb";
	ExecutorService taskExecutor = null;
	Variable var;
	
	@Before
	public void setUp() throws IOException{
		NetcdfFile file = NetcdfFile.open(location);		
		var = file.findVariable(varName);
		
		//GridDataset gds = new GridDataset(new NetcdfDataset(file));
		//var = gds.findGridByName(varName).getVariable();
	}
	
	@Test
	public void VariableSectionReaderAndExecutorServiceTest() throws InterruptedException, ExecutionException{
		
		int nProc = Runtime.getRuntime().availableProcessors();
		taskExecutor = Executors.newFixedThreadPool(nProc);
		//taskExecutor = Executors.newFixedThreadPool(2);
		List<Future<Array>> list = new ArrayList<Future<Array>>();
		//We have to know the shape of the variabe beforehand
		//Tha would need an extra operation that must be counted too.
		//int timesPerOperation = 5000;
		int totalTimes = 98128;		
		int timesPerOperation = 10000;
		int currentTime =0;
			
		
		long start = System.currentTimeMillis();
		while( currentTime < totalTimes  ){

			if(currentTime+timesPerOperation > totalTimes ){
				timesPerOperation = totalTimes - currentTime;
			}

			int[] origin  = new int[]{currentTime,0,0};
			int[] section = new int[]{timesPerOperation, 1, 1};
			
			//System.out.println( "Origin:  "+Arrays.toString(origin));
			//System.out.println( "Section: "+Arrays.toString(section));
			
			Callable<Array> worker = VariableSectionReader.factory(var, origin, section);
			Future<Array> submit = taskExecutor.submit(worker);
			list.add(submit);
			
			//origin[0] += timesPerOperation;						
			currentTime+=timesPerOperation;
			
		}
		
		long bRead =0;
		for(Future<Array> future : list){
			Array d = future.get();
			bRead+=d.getSizeBytes();
		}
		
		taskExecutor.shutdown();
		System.out.println("All done. "+bRead+" bytes read in..."+ (System.currentTimeMillis() - start )+" ms.");
	}	
	
}
