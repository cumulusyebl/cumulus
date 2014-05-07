package org.apache.hadoop.hdfs.server.namenode;
import java.util.*;
import java.io.*;

/**
* @author tony
*/
public class HeaderBuffer {
	
	/**new hash and entry links**/
	
	HashMap<INodeFile,BufferData> new_hash=null;
	LinkedList<BufferData> new_entries=null;
	
	File bufferStorage=null;
	
	RandomAccessFile storage = null;
	
	protected static HeaderBuffer _instance=null; 
	
	long bufferMaxSize;
	
	/**indicates the position of the buffer to be added in the storageFile**/
	static long bufferposition=0; 
	
	public void setBufferPosition(long bufferpos)
	{
		this.bufferposition = bufferpos+1;
	}
	private HeaderBuffer(long buffermaxsize,File storageFile,long maxoffset)
	{
		this.new_hash = new HashMap<INodeFile,BufferData>();
		this.new_entries = new LinkedList<BufferData>();
		
		this.bufferMaxSize = buffermaxsize;
		this.bufferStorage = storageFile;
		this.bufferposition = maxoffset+1; 
		try 
		{
		   this.storage=new RandomAccessFile(this.bufferStorage,"rw");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		NameNode.LOG.info("HeaderBuffer Created:buffermaxsize: "+this.bufferMaxSize+"bufferposition"+
		this.bufferposition+"--------------------TONY");
	}
	
	/**
	 * get the Singleton Instance
	 * @param size  the max size of the buffer
	 * @param storageFile  the storageFile on the disk
	 * @return
	 */
	public static HeaderBuffer Instance(long size,File storageFile,long maxoffset)
	{
		if(_instance==null)
		{
			_instance=new HeaderBuffer(size,storageFile,maxoffset);
		}
		return _instance;
	}
	
	/***
	 * replace the oldFile in buffer
	 * @param oldFile
	 * @param newFile
	 */
	synchronized public void change(INodeFile oldFile,INodeFile newFile){
		NameNode.LOG.info("In HeaderBuffer.change()--------------TONY");
		newFile.header_offset = oldFile.header_offset;
		
		BufferData temp = new_hash.get((INode)oldFile);
		if(temp == null)
		{
			if(new_entries.size()>=bufferMaxSize)
			{
				/**the size of the new_entries has overhead**/
				temp=new_entries.getLast();
				new_entries.removeLast();
				new_hash.remove(temp.getIndexFile());
				/**remove the last and serialize to the storagefile**/
				serializeToFile(temp);
				temp=null;
			}
			BufferData node = new BufferData(newFile,oldFile.fetchBufferData());
			new_entries.addFirst(node);
			new_hash.put(newFile, node);
		}
		else
		{
			new_hash.remove(oldFile);
			new_entries.remove(temp);
			
			BufferData node = new BufferData(newFile,oldFile.fetchBufferData());
			
			new_entries.addFirst(node);
			new_hash.put(newFile, node);
		}
	}
	
	/***
	 * remove the BufferData from the buffer
	 * @param indexFile
	 */
	synchronized public void remove(INodeFile indexFile)
	{
		indexFile.header_offset = -1;
		BufferData removed = new_hash.get(indexFile);
		if(removed != null)
		{
			new_hash.remove(indexFile);
			new_entries.remove(removed);
		}
		removed = null;
	}
	/**
	 * put the BufferData(filepath,head) into the buffer.
	 * if the size>bufferMaxSize then replace some BufferData according to LRU
	 * @param filePath
	 * @param head
	 */
	synchronized public void put(INodeFile indexfile,byte[] head) {
		NameNode.LOG.info("HeaderBuffer put "+indexfile+" --------------TONY");
		BufferData temp=new_hash.get(indexfile);
		NameNode.LOG.info("HeaderBUffer put BufferData"+temp+"-------TONY"); 
		
		/*filePath exists*/
		if(temp!=null){
			/**the head of the indexfile already exists,
			 * move the buffer to the head of the list**/
			new_entries.remove(temp);
			new_entries.addFirst(temp);
		}
		else
		{
			if(new_entries.size()>=bufferMaxSize){
					/**the size of the new_entries has overhead**/
					temp=new_entries.getLast();
					new_entries.removeLast();
					new_hash.remove(temp.getIndexFile());
					temp=null;
				}
				BufferData node=new BufferData(indexfile,head);
				NameNode.LOG.info("HeaderBuffer put before add node:"+node.toString()+"------------TONY");
				new_entries.addFirst(node);
				new_hash.put(indexfile,node);
				serializeToFile(node);
				
				NameNode.LOG.info("HeaderBuffer after put node:"+node.toString()+"----------TONY");
		}
	}
	
	
	/**
	 * serialize the node to disk
	 * @throws IOException 
	 */
	void serializeToFile(BufferData node) {
		NameNode.LOG.info("HeaderBuffer put serialize------TONY");
		try {
				/**header_offset == -1 means the inodefile has never been buffered**/
				if(node.getIndexFile().header_offset==-1){
					node.getIndexFile().header_offset=this.bufferposition;
					this.bufferposition++;
				}
				node.write(this.storage);
				NameNode.LOG.info("HeaderBuffer put in serialize-------TONY");
			} catch (IOException e) {
				
				e.printStackTrace();
			}
	}
	
	
	/**
	 * get the BufferData with the filepath,if not exist in the entries,
	 * find from the bufferStorage file.
	 * @param filePath
	 * @return
	 */
	synchronized public BufferData get(INode indexfile)
	{
		NameNode.LOG.info("HeaderBuffer get "+indexfile+" "+indexfile.hashCode()+"-----------TONY");
		BufferData ans=new_hash.get(indexfile);
		/**get target from the entries**/
		if(ans!=null)
		{
			new_entries.remove(ans);
			new_entries.addFirst(ans);
			return ans;
		}
		else
		{	/**get the buffer from the storagefile**/
		    ans=deserializeFromFile((INodeFile)indexfile);
		    if(new_entries.size()>=bufferMaxSize){
				/**the size of the new_entries has overhead**/
		    	BufferData temp=new_entries.getLast();
				new_entries.removeLast();
				new_hash.remove(temp.getIndexFile());
				temp=null;
			}
		    new_hash.put(ans.getIndexFile(), ans);
		    new_entries.addFirst(ans);
		    return ans;
		}
	}
	 
	 BufferData deserializeFromFile(INodeFile indexfile) {
		 	BufferData ans=new BufferData(indexfile);
			try 
			{
				NameNode.LOG.info("HeaderBuffer get  Header from bufferStorage-----TONY");
				
				ans.readFields(this.storage);
				
				NameNode.LOG.info("HeaderBuffer get after getting Header"+"----------TONY");
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
			return ans;
	 }
}

