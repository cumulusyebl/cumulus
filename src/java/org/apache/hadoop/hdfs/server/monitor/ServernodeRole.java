
package org.apache.hadoop.hdfs.server.monitor;
/**
 * author: xianyu
 * date: 2014-04-07
 **/

public enum ServernodeRole {
	NAMENODE	("NameNode"), 
	DATANODE	("DataNode");
	
	private String description;
	private ServernodeRole(String arg){
		description = arg;
	}
	
	public String toString(){
		return description;
	}
}