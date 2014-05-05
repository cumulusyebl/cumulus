
package org.apache.hadoop.hdfs.server.monitor;
/**
 * author: xianyu
 * data: 2014-04-07
 * */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;

public class ServernodeStator{
	private ServernodeCPUStator cpuStator;
	private ServernodeMEMStator memStator;
	private ServernodeNETStator netStator;
	private ServernodeIOStator ioStator;
	
	public ServernodeStator(ServernodeRole role){
		this(new HdfsConfiguration(), role);
	}
	
	public ServernodeStator(Configuration conf, ServernodeRole role){
//		System.out.println("[INFO by xianyu]Stator role: " + role.toString());
		cpuStator = new ServernodeCPUStator();
		memStator = new ServernodeMEMStator();
		netStator = new ServernodeNETStator();
		ioStator = new ServernodeIOStator(conf, role);
	}
	
	public void activate(){
		cpuStator.activate();
		memStator.activate();
		netStator.activate();
		ioStator.activate();
	}
	
	public ServernodeCPUStatus getCPUStatus(){
		return cpuStator.getStatus();
	}
	
	public ServernodeMEMStatus getMEMStatus(){
		return memStator.getStatus();
	}
	
	public ServernodeNETStatus[] getNETStatus(){
		return netStator.getStatus();
	}
	
	public ServernodeIOStatus[] getIOStatus(){
		return ioStator.getStatus();
	}
	
	//for test
	public static void main(String[] args) throws Exception{
		new ServernodeStator(ServernodeRole.NAMENODE).activate();
//		new ServernodeStator(ServernodeRole.DATANODE).activate();
		Thread.sleep(500*1000);
	}
}
