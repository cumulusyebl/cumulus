package org.apache.hadoop.hdfs.protocol;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;

public class LocatedBlockWithCodingFactor extends LocatedBlock implements Writable{
	private int[] factors;
	public LocatedBlockWithCodingFactor(){
		super();
		this.factors = new int[0];
	}
	public LocatedBlockWithCodingFactor(LocatedBlock b, int[] factors){
		//TODO: remove this class
//		super(b);
		this.factors = factors;
	}
	public int[] getFactors(){
		return this.factors;
	}
	

	public void write(DataOutput out) throws IOException {
		 super.write(out);
		 out.writeInt(factors.length);
		 for (int i = 0; i < factors.length; i++) {
		     out.writeInt(factors[i]);
		    }
		  }

	public void readFields(DataInput in) throws IOException {
			  super.readFields(in);
			  
			 factors = new int[in.readInt()];
		    for (int i = 0; i < factors.length; i++) {
		    	factors[i] = in.readInt();
		    }
		  }
	public static LocatedBlockWithCodingFactor read(DataInput in) throws IOException {
			final LocatedBlockWithCodingFactor lbf = new LocatedBlockWithCodingFactor();
			lbf.readFields(in);
			return lbf;
		}

	}
