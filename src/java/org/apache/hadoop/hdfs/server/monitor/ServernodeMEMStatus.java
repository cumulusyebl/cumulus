
package org.apache.hadoop.hdfs.server.monitor;
/**
 * author: xianyu
 * date: 2014-04-07
 * */

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import org.apache.hadoop.io.Writable;

/**
 * ServernodeMEMStatus represents the status of memory.
 * mem_rate means how many memory is in-use, in percent.
 */
public class ServernodeMEMStatus implements Serializable, Writable{
	private static final long serialVersionUID = 1L;
	
	private double cur_mem_rate;
	private double avg_mem_rate;
	private double max_mem_rate;
	private double min_mem_rate;
	
	public ServernodeMEMStatus(){
		cur_mem_rate = 0;
		avg_mem_rate = 0;
		max_mem_rate = 0;
		min_mem_rate = 0;
	}
	
	public ServernodeMEMStatus(ServernodeMEMStatus that){
		this.cur_mem_rate = that.cur_mem_rate;
		this.avg_mem_rate = that.avg_mem_rate;
		this.max_mem_rate = that.max_mem_rate;
		this.min_mem_rate = that.min_mem_rate;
	}
	
	public String toString(){
		return String.format("[mem status]\n" + 
				"\tcur-rate: %.2f%%; avg-rate: %.2f%%\n" + 
				"\tmax-rate: %.2f%%; min-rate: %.2f%%\n", 
					cur_mem_rate * 100, avg_mem_rate * 100, 
					max_mem_rate * 100, min_mem_rate * 100);
	}
	
	public String toDump(){
		return String.format("%.2f%% %.2f%% %.2f%% %.2f%%", 
				cur_mem_rate * 100, avg_mem_rate * 100, 
				max_mem_rate * 100, min_mem_rate * 100);
	}
	
	//to get cur_mem_rate of mem
	public double getCurMEMRate(){
		return cur_mem_rate;
	}
	//to set cur_mem_rate of mem
	public void setCurMEMRate(double r){
		this.cur_mem_rate = r;
	}
	
	//to get avg_mem_rate of mem
	public double getAvgMEMRate(){
		return avg_mem_rate;
	}
	//to set avg_mem_rate of mem
	public void setAvgMEMRate(double r){
		this.avg_mem_rate = r;
	}
	
	//to get max_mem_rate of mem
	public double getMaxMEMRate(){
		return max_mem_rate;
	}
	//to set max_mem_rate of mem
	public void setMaxMEMRate(double r){
		this.max_mem_rate = r;
	}
	
	//to get min_mem_rate of mem
	public double getMinMEMRate(){
		return min_mem_rate;
	}
	//to set min_mem_rate of mem
	public void setMinMEMRate(double r){
		this.min_mem_rate = r;
	}
	
	//implements the Writable interface
	public void write(DataOutput out) throws IOException {
		out.writeDouble(cur_mem_rate);
		out.writeDouble(avg_mem_rate);
		out.writeDouble(max_mem_rate);
		out.writeDouble(min_mem_rate);
	}
	public void readFields(DataInput in) throws IOException {
		cur_mem_rate = in.readDouble();
		avg_mem_rate = in.readDouble();
		max_mem_rate = in.readDouble();
		min_mem_rate = in.readDouble();
	}
	public static ServernodeMEMStatus read(DataInput in) throws IOException{
		ServernodeMEMStatus s = new ServernodeMEMStatus();
		s.readFields(in);
		
		return s;
	}
}
