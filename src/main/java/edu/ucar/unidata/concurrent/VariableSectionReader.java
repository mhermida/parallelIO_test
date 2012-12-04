package edu.ucar.unidata.concurrent;

import java.io.IOException;
import java.util.concurrent.Callable;

import ucar.ma2.Array;
import ucar.ma2.InvalidRangeException;
import ucar.nc2.Variable;

public class VariableSectionReader implements Callable<Array> {

	private final Variable v;
	private int[] origin;
	private int[] wantedShape;
	
	private VariableSectionReader(Variable v, int[] origin, int[] wantedShape){
		this.v=v;
		this.origin=origin;
		this.wantedShape=wantedShape;
	}
	
	public Array call() throws IOException, InvalidRangeException{
		synchronized(v){
			return v.read(origin, wantedShape);
		}
	}
	
	public static VariableSectionReader factory(Variable v, int[] origin, int[] wantedShape){
		return new VariableSectionReader(v, origin, wantedShape);
	}
}
