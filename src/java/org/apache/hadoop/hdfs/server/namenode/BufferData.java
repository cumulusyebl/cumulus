package org.apache.hadoop.hdfs.server.namenode;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.Writable;
/**
 * 
 * @author tony
 *
 */
public class BufferData implements Writable{
		


		/*the absolute path of a INodeFile*/
		private Text filePath;	
		
		/*the head vector of the INodeFile*/
		private BytesWritable head=null;
		
		public BufferData(){
			this.filePath=new Text();
			this.head=new BytesWritable();
		}
		public BufferData(String filePath,byte[] head){
			this.filePath=new Text(filePath);
			this.head=new BytesWritable(head);
		}
		
		public void setFilePath(String filePath){
			this.filePath=new Text(filePath);
		}
		public String getFilePath(){
			return this.filePath.toString();
		}
		
		public void setHead(byte[] head){
			this.head=new BytesWritable(head);
		}
		public byte[] getHead(){
			return this.head.copyBytes();
		}
		@Override
		public void write(DataOutput out) throws IOException {
			// TODO Auto-generated method stub
			filePath.write(out);
			head.write(out);
		}
		@Override
		public void readFields(DataInput in) throws IOException {
			// TODO Auto-generated method stub
			filePath.readFields(in);
			head.readFields(in);
		}
		public String toString(){
			return this.filePath.toString()+"----------"+this.head.toString();
		}
}
