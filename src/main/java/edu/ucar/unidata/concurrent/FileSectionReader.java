/**
 * 
 */
package edu.ucar.unidata.concurrent;

import java.io.IOException;
import java.util.concurrent.Callable;

import ucar.ma2.Array;
import ucar.ma2.InvalidRangeException;
import ucar.nc2.NetcdfFile;

/**
 * @author mhermida
 *
 */
public final class FileSectionReader implements Callable<Array> {
	
	private String fileLocation;
	private final String varName;
	private final int[] origin;
	private final int[] wantedShape;
	
	private FileSectionReader(String fileLocation, String varName, int[] origin, int[] wantedShape){
		this.fileLocation=fileLocation;
		this.varName = varName;
		this.origin=origin;
		this.wantedShape=wantedShape;
	}

	public Array call() throws IOException, InvalidRangeException{
		
		NetcdfFile file = NetcdfFile.open(fileLocation);		
		Array data = file.findVariable(varName).read(origin, wantedShape);
		file.close();
		
		return data;
	}
	
	public static FileSectionReader factory(String fileLocation, String varName, int[] origin, int[] wantedShape){
		
		return new FileSectionReader(fileLocation, varName, origin, wantedShape);
	}
}
