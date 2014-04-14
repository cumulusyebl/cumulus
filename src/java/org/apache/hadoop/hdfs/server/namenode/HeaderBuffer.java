package org.apache.hadoop.hdfs.server.namenode;
import java.util.*;
import java.io.*;

/**
* @author tony
*/
public class HeaderBuffer {

	/*hash for get the data in O(1)*/
	HashMap<String,BufferData> hash=null;
	
	LinkedList<BufferData> entries=null;
	
	Set<String> pathset=null;
	
	File bufferStorage=null;
	
	protected static HeaderBuffer _instance=null; 
	
	long bufferMaxSize;
	
	
	private HeaderBuffer(long buffermaxsize,File storageFile)
	{
		this.hash=new HashMap<String,BufferData>();
		this.entries=new LinkedList<BufferData>();
		this.pathset=new HashSet<String>();
		this.bufferMaxSize=buffermaxsize;
		this.bufferStorage=storageFile;
		NameNode.LOG.info("HeaderBuffer Created--------------------TONY");
	}
	/**
	 * get the Singleton Instance
	 * @param size  the max size of the buffer
	 * @param storageFile  the storageFile on the disk
	 * @return
	 */
	public static HeaderBuffer Instance(long size,File storageFile)
	{
		if(_instance==null)
		{
			_instance=new HeaderBuffer(size,storageFile);
		}
		return _instance;
	}
	/**
	 * put the BufferData(filepath,head) into the buffer.
	 * if the size>bufferMaxSize then replace some BufferData according to LRU
	 * @param filePath
	 * @param head
	 */
	synchronized public void put(String filePath,byte[] head) 
	{
		NameNode.LOG.info("HeaderBuffer put--------------TONY");
		BufferData temp=hash.get(filePath);
		NameNode.LOG.info("HeaderBUffer put BufferDat"+temp+"-------TONY"); 
		/*filePath exists*/
		if(temp!=null){
			entries.remove(temp);
			entries.addFirst(temp);
		}
		else{
			if(entries.size()>=bufferMaxSize){
				temp=entries.getLast();
				entries.removeLast();
				hash.remove(temp.getFilePath());
			}
			BufferData node=new BufferData(filePath,head);
			NameNode.LOG.info("HeaderBuffer put before add node:"+node.toString()+"------------TONY");
			entries.addFirst(node);
			hash.put(filePath, node);
			NameNode.LOG.info("HeaderBuffer after put node:"+node.toString()+"----------TONY");
			/*
			 * serialize the node to disk
			 */
			NameNode.LOG.info("HeaderBuffer put serialize------TONY");
			if(!pathset.contains(filePath)){
				pathset.add(filePath);
				try {
					FileOutputStream fout=new FileOutputStream(bufferStorage,true);
					NameNode.LOG.info("HeaderBuffer put in serialize-------TONY");
					DataOutputStream out=new DataOutputStream(fout);
					node.write(out);
					out.close();
					fout.close();
				} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
				// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}
	
	/**
	 * get the BufferData with the filepath,if not exist in the entries,
	 * find from the bufferStorage file.
	 * @param filePath
	 * @return
	 */
	 BufferData get(String filePath)
	{
		 NameNode.LOG.info("HeaderBuffer get-----------TONY");
		BufferData ans=hash.get(filePath);
		if(ans!=null)
		{
			entries.remove(ans);
			entries.addFirst(ans);
			return ans;
		}
		else
		{
			/*
			 *  get the ans from the storagefile
			 */
		    ans=new BufferData();
			boolean getAns=false;
			try {
				NameNode.LOG.info("HeaderBuffer get get Header from bufferStorage-----TONY");
				FileInputStream fin=new FileInputStream(bufferStorage);
				DataInputStream in=new DataInputStream(fin);
				while(in.available()!=0){
					ans.readFields(in);
					if(filePath.equals(ans.getFilePath())){
						getAns=true;
						break;
					}
				}
				NameNode.LOG.info("HeaderBuffer get after getting Header"+getAns+"----------TONY");
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
			if(getAns==true){
				return ans;
			}else{
				return null;  //can't get the ans from bufferStorage
			}
		}
	}
}

