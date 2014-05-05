
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
 * ServernodeCPUStatus represents the status of cpu.
 * cpu_rate means how often cpu is in-use, in percent.
 * */
public class ServernodeCPUStatus implements Serializable, Writable{
	private static final long serialVersionUID = 1L;
	
	private double cur_cpu_rate;
	private double avg_cpu_rate;
	private double max_cpu_rate;
	private double min_cpu_rate;
	
	public ServernodeCPUStatus(){
		cur_cpu_rate = 0;
		avg_cpu_rate = 0;
		max_cpu_rate = 0;
		min_cpu_rate = 0;
	}
	
	public ServernodeCPUStatus(ServernodeCPUStatus that){
		this.cur_cpu_rate = that.cur_cpu_rate;
		this.avg_cpu_rate = that.avg_cpu_rate;
		this.max_cpu_rate = that.max_cpu_rate;
		this.min_cpu_rate = that.min_cpu_rate;
	}
	
	public String toString(){
		return String.format("[cpu status]\n" + 
				"\tcur-rate: %.2f%%; avg-rate: %.2f%%\n" + 
				"\tmax-rate: %.2f%%; min-rate: %.2f%%\n", 
					cur_cpu_rate * 100, avg_cpu_rate * 100, 
					max_cpu_rate * 100, min_cpu_rate * 100);
	}
	
	public String toDump(){
		return String.format("%.2f%% %.2f%% %.2f%% %.2f%%", 
				cur_cpu_rate * 100, avg_cpu_rate * 100, 
				max_cpu_rate * 100, min_cpu_rate * 100);
	}
	
	//to get the cur_cpu_rate of a cpu
	public double getCurCPURate(){
		return cur_cpu_rate;
	}
	//to set the cur_cpu_rate of a cpu
	public void setCurCPURate(double r){
		this.cur_cpu_rate = r;
	}
	
	//to get the avg_cpu_rate of a cpu
	public double getAvgCPURate(){
		return avg_cpu_rate;
	}
	//to set the avg_cpu_rate of a cpu
	public void setAvgCPURate(double r){
		this.avg_cpu_rate = r;
	}
	
	//to get the max_cpu_rate of a cpu
	public double getMaxCPURate(){
		return max_cpu_rate;
	}
	//to set the max_cpu_rate of a cpu
	public void setMaxCPURate(double r){
		this.max_cpu_rate = r;
	}
	
	//to get the min_cpu_rate of a cpu
	public double getMinCPURate(){
		return min_cpu_rate;
	}
	//to set the min_cpu_rate of a cpu
	public void setMinCPURate(double r){
		this.min_cpu_rate = r;
	}

	//implements the Writable interface
	public void write(DataOutput out) throws IOException {
		out.writeDouble(cur_cpu_rate);
		out.writeDouble(avg_cpu_rate);
		out.writeDouble(max_cpu_rate);
		out.writeDouble(min_cpu_rate);
	}
	public void readFields(DataInput in) throws IOException {
		cur_cpu_rate = in.readDouble();
		avg_cpu_rate = in.readDouble();
		max_cpu_rate = in.readDouble();
		min_cpu_rate = in.readDouble();
	}
	public static ServernodeCPUStatus read(DataInput in) throws IOException{
		ServernodeCPUStatus s = new ServernodeCPUStatus();
		s.readFields(in);
		
		return s;
	}
}
