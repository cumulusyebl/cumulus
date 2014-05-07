package org.apache.hadoop.hdfs.server.namenode;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;

import org.apache.hadoop.io.*;
/**
 * @author tony
 */
public class BufferData {
		
		/*the INodeFile used as index*/
		private INodeFile indexFile;
		
		/*the head vector of the INodeFile*/
		private byte[] head=null;
		
		/**
		 * new constructor
		 * @param ifile 	the InodeFile used as index
		 * @param head		the coding matrix
		 */
		public BufferData(INodeFile ifile,byte[] head)
		{
			this.indexFile= ifile;
			this.head= head;
		}
		
		public BufferData(INodeFile ifile)
		{
			this.indexFile= ifile;
			this.head=new byte[64];
		}
		
		public void setHead(byte[] head){
			this.head=head;
		}
		public byte[] getHead(){
			return this.head;
		}
		
		/**
		 * new get&set for indexFile
		 * @param ifile
		 */
		public void setIndexFile(INodeFile ifile)
		{
			this.indexFile=ifile;
		}
		
		public INodeFile getIndexFile()
		{
			return this.indexFile;
		}
		
		public void write(RandomAccessFile out) throws IOException {
			out.seek(0);
			out.seek(indexFile.header_offset*64);
			out.write(this.head);
		}
		
		public void readFields(RandomAccessFile in) throws IOException {
			 in.seek(0);
			 in.seek(indexFile.header_offset*64);
			 in.read(this.head);
		}
		public String toString(){
			return "BufferData------"+Arrays.hashCode(this.head);
		}
}
