package edu.ucar.unidata.concurrent;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import ucar.ma2.Array;
import ucar.ma2.DataType;
import ucar.ma2.InvalidRangeException;
import ucar.nc2.Variable;
import ucar.nc2.dataset.NetcdfDataset;
import ucar.nc2.dt.GridDatatype;
import ucar.nc2.dt.grid.GridDataset;

public class VariableSectionChannelReaderDemo {
	
	private String location ="/home/mhermida/data/meteogalicia/roms_002_20121002_0000.nc";
	private String varName = "temp"; // 97x476x401x15
	private GridDataset gds;
	private GridDatatype grid;
	
	@Before
	public void setUp() throws IOException{
		
		gds = new GridDataset(NetcdfDataset.openDataset(location) );
		grid = gds.findGridDatatype(varName);
	}
	
	@Test
	public void baseBenchmark() throws IOException, InvalidRangeException{
		
		long start = System.currentTimeMillis();		
		//Array allData = grid.getVariable().read();
		int[] origin = new int[]{0,0,0,0};
		int[] shape = new int[]{1,15,401,476};
		//int[] shape = new int[]{1,1,1,1};
		long readData =0;
		
		for(int i=0; i<97; i++){
			origin[0]=i;
			//for(int j=0; j<15; j++){

				//origin[1]=j;
				Array d = grid.getVariable().read(origin, shape);
				readData+=d.getSizeBytes();
				System.out.println(d.getDouble(0));

			//}			
		}
		
		System.out.println(readData+" bytes read in: "+(System.currentTimeMillis() - start));
	}
	
	
	@Test
	public void variableSectionChannelAndExecutor() throws IOException, InterruptedException, ExecutionException{
		
		Variable var = grid.getVariable().getOriginalVariable();
		DataType dType = var.getDataType();
		int[] varShape = var.getShape();
		
		gds.close();
		
		//Object that contains variable is not visible 
		//N3header.Vinfo vinfo = (N3header.Vinfo) var.getSPobject();
		long varStartPos = 74827420;
		
		Path path = Paths.get(location);
		final int nThreads = Runtime.getRuntime().availableProcessors();
		final Set<StandardOpenOption> options = new TreeSet<>();
		options.add(StandardOpenOption.READ);
		ExecutorService taskExecutor = Executors.newFixedThreadPool(nThreads);
		
		long start = System.currentTimeMillis();
		//List<Future<ByteBuffer>> list = new ArrayList<>();
		List<Future<Future<Integer>>> list = new ArrayList<>();
		List<ByteBuffer> pool = new ArrayList<>();
		//CompletionService<ByteBuffer> resultPool = new ExecutorCompletionService<>(taskExecutor);
		
		try(AsynchronousFileChannel afc = AsynchronousFileChannel.open(path, options, taskExecutor ) ) {
		
			//int wantedBytes = 8*401*476;
			int wantedBytes = 93147496;

			for(int i=0; i<97; i++ ){
				//for(int j=0; j<15; j++){
					ByteBuffer buffer = ByteBuffer.allocate(8*15*401*476); //one time and vertical level slice
					pool.add(buffer);
					Callable<Future<Integer>> worker = VariableSectionChannelReader.factory(afc, varStartPos, buffer);
					varStartPos+=wantedBytes;
					
					//resultPool.submit(worker);					
					Future<Future<Integer>> future = taskExecutor.submit(worker); 
					list.add(future);
				//}
				
			}
			
			
			for(Future<Future<Integer>> future : list){

				boolean done = future.get().isDone();
				System.out.println("Inner task done: "+done);
				done = future.isDone();
				System.out.println("Submitted task done: "+done);
			}			
			
			taskExecutor.shutdown();
			
			taskExecutor.awaitTermination(10, TimeUnit.SECONDS);
			
			if(taskExecutor.isTerminated() ){
				System.out.println( "Finally done!!" );
			}
			
			long readBytes =0;
			for(ByteBuffer b : pool){
				//b.flip();
				//b.rewind();
				System.out.println( b.getDouble(0) );
				readBytes+=b.array().length;
				
			}
			System.out.println( readBytes +" read bytes in: "+(System.currentTimeMillis() - start) );

		}			
				
	}
	
	@After
	public void tearDown() throws IOException{
		gds.close();
		grid = null;
	}

}
