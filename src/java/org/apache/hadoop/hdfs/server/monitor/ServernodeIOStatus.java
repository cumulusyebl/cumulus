
package org.apache.hadoop.hdfs.server.monitor;
/**
 * author: xianyu
 * date: 2014-04-07
 * */

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * ServernodeIOStatus represents the status of IO.
 * io_rate means how busy io is, that's to say, 
 * how often read/write happens in a specified time slot.
 * */
public class ServernodeIOStatus implements Serializable, Writable{
	private static final long serialVersionUID = 1L;
	
	private String diskname;
	private double cur_io_rate;
	private double avg_io_rate;
	private double max_io_rate;
	private double min_io_rate;
	
	public ServernodeIOStatus(){
		this("null");
	}
	
	public ServernodeIOStatus(String name){
		diskname = name;
		cur_io_rate = 0;
		avg_io_rate = 0;
		max_io_rate = 0;
		min_io_rate = 0;
	}
	
	public ServernodeIOStatus(ServernodeIOStatus that){
		this.diskname = that.diskname;
		this.cur_io_rate = that.cur_io_rate;
		this.avg_io_rate = that.avg_io_rate;
		this.max_io_rate = that.max_io_rate;
		this.min_io_rate = that.min_io_rate;
	}
	
	public String toString(){
		return String.format("[io status]\n" + 
				"\tdiskname: %s\n" + 
				"\tcur-rate: %.2f%%; avg-rate: %.2f%%\n" + 
				"\tmax-rate: %.2f%%; min-rate: %.2f%%\n", 
					diskname, 
					cur_io_rate * 100, avg_io_rate * 100, 
					max_io_rate * 100, min_io_rate * 100);
	}
	
	public String toDump(){
		return String.format("%s %.2f%% %.2f%% %.2f%% %.2f%%", 
				diskname, 
				cur_io_rate * 100, avg_io_rate * 100, 
				max_io_rate * 100, min_io_rate * 100);
	}
	
	//to get the diskname
	public String getDiskname(){
		return diskname;
	}
	//to set the diskname
	public void setDiskname(String name){
		this.diskname = name;
	}
	
	//to get the cur-rate of io
	public double getCurIORate(){
		return cur_io_rate;
	}
	//to set the cur-rate of io
	public void setCurIORate(double r){
		this.cur_io_rate = r;
	}
	
	//to get the avg-rate of io
	public double getAvgIORate(){
		return avg_io_rate;
	}
	//to set the avg-rate of io
	public void setAvgIORate(double r){
		this.avg_io_rate = r;
	}
	
	//to get the max-rate of io
	public double getMaxIORate(){
		return max_io_rate;
	}
	//to set the max-rate of io
	public void setMaxIORate(double r){
		this.max_io_rate = r;
	}
	
	//to get the min-rate of io
	public double getMinIORate(){
		return min_io_rate;
	}
	//to set the min-rate of io
	public void setMinIORate(double r){
		this.min_io_rate = r;
	}
	
	//implements Writable interface
	public void write(DataOutput out) throws IOException {
		Text.writeString(out, diskname);
		out.writeDouble(cur_io_rate);
		out.writeDouble(avg_io_rate);
		out.writeDouble(max_io_rate);
		out.writeDouble(min_io_rate);
	}
	public void readFields(DataInput in) throws IOException {
		diskname = Text.readString(in);
		cur_io_rate = in.readDouble();
		avg_io_rate = in.readDouble();
		max_io_rate = in.readDouble();
		min_io_rate = in.readDouble();
	}
	public static ServernodeIOStatus read(DataInput in) throws IOException{
		ServernodeIOStatus s = new ServernodeIOStatus();
		s.readFields(in);
		
		return s;
	}
}
