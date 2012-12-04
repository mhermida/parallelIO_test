package edu.ucar.unidata.concurrent;

import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import ucar.ma2.DataType;

public class VariableSectionChannelReader implements Callable<Future<Integer>> {

	private AsynchronousFileChannel asynchronousFileChannel;
	private long start;
	private ByteBuffer buffer;
	
	private VariableSectionChannelReader(AsynchronousFileChannel asynchronousFileChannel, long start, ByteBuffer buffer ){
		this.asynchronousFileChannel=asynchronousFileChannel;
		this.start =start;
		this.buffer = buffer;
	}
	
	public Future<Integer> call() throws InterruptedException, ExecutionException{
	
		return asynchronousFileChannel.read(buffer, start);
 		
	}
	
	public static VariableSectionChannelReader factory(AsynchronousFileChannel asynchronousFileChannel, long start, ByteBuffer buffer){
		return new VariableSectionChannelReader(asynchronousFileChannel, start, buffer);
	}
}
