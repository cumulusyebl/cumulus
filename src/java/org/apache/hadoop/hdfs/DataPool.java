package org.apache.hadoop.hdfs;


import java.io.IOException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


public class DataPool {
	
	BlockReader br;
	int bufferSize = 1024*1024*2; // 2MB
	byte[] buffer;
    int unDecodedStart = 0, unDecodedEnd = 0;
    int dataLength = 0;//remaining amount of data in  buffer
    int threshold = bufferSize/2;
    Lock lock = new ReentrantLock();
    Condition enough = lock.newCondition(); //whether enough data
    Condition preFetch = lock.newCondition();//whether need to prefetch
    boolean rich = true;
    int lastLength = 0;
    boolean run = true;
    int position = 0; //current offset into blk
    public int readedPosition;
    public boolean error = false;
    
   /*
    public static void main(String[] args) throws IOException{
    	File file = new File("/home/czl/Desktop/OP.mkv");
    	InputStream in = new FileInputStream(file);
    	DataPool dp = new DataPool(in, 1024*1024*16);
    	
    	File file2 = new File("/home/czl/Desktop/OP2.mkv");
    	OutputStream out = new FileOutputStream(file2);
    	
    	byte[] b = new byte[1024*1024*4];
    	int c;
    	while((c=dp.getData(b, 1024*1024*4))!=0){
    		out.write(b, 0, c);
    	}
    	
    	out.flush();
    	out.close();
    }
    */
	
	public DataPool (BlockReader br, int startOffset, int bufferSize) throws IOException {
		this.br = br;
		this.bufferSize = bufferSize+1;
		buffer = new byte[this.bufferSize];
		position = startOffset;
		readedPosition = startOffset;
		dataLength = br.read(buffer, 0, this.bufferSize);
		//DFSClient.LOG.info("dataLength: "+dataLength);
		unDecodedEnd = dataLength;
		
		new Thread(new Monitor()).start();//always running to check remaining amount of data
	}
	
	/*
	 * skip to 
	 * difficult because of we pre-fetch
	 */
	public void seekTo(long skip) throws IOException{
		lock.lock();
		try {
			if(skip>position){
				br.skip(skip-position);
				position = (int) skip;
				readedPosition = (int) skip;
				dataLength = 0;
				unDecodedStart = unDecodedEnd = 0;
				rich = false;
				lastLength = 0;
		}
		else {
			if(skip<position && skip>readedPosition){
				int skipLength = (int) (skip - readedPosition);
				readedPosition = (int) skip;
				dataLength -= skipLength;
				unDecodedStart = (int) ((unDecodedStart+skip)%bufferSize);
				rich = false;
				lastLength = 0;
			}
		}
		} catch (Exception e) {
			// TODO: handle exception
		}
		finally{
			lock.unlock();
		}
	}
	/*
	 * decoding needs data of length
	 * length should be smaller than size of buff!!!!
	 * when comes to EOF,length should be calculated instead of use size of buff directely
	 */
	 public int getData(byte[] buff, int length){
		if(length>buff.length){
			length = buff.length;
		}
		lock.lock();
		lastLength = length;
		//System.out.println("rich: "+rich);
		
		try {
			while(length > dataLength && !rich && run && dataLength!=bufferSize){
				//no enough data , fetch
				enough.await();
			} 
			//System.out.println("rich: "+rich+"  run: "+run);
			//tried to fetch but still no enough data, may reached EOF
			if (length>dataLength){
				length=dataLength;
			}

			//now dataLength is larger length
		    if(unDecodedEnd > unDecodedStart){
		    	/*end is right to start
		    	      start              end
		    	        |                 | 
		    	-----------------------------------
		    	|                                 |
		    	-----------------------------------
		    	*/
		    	System.arraycopy(buffer, unDecodedStart, buff, 0, length);
		    	unDecodedStart += length;
		    }
		    else {
				if(bufferSize-unDecodedStart>=length){
					System.arraycopy(buffer, unDecodedStart, buff, 0, length);
					unDecodedStart += length;
				}
				else {
				/*
		    	    end                       start
		    	     |                          |
		    	 -----------------------------------
                 |		    	                   |
		    	 ----------------------------------- 
		    	  
		    	 */
					System.arraycopy(buffer, unDecodedStart, buff, 0, bufferSize-unDecodedStart);
					System.arraycopy(buffer, 0, buff, bufferSize-unDecodedStart, length-bufferSize+unDecodedStart);
					unDecodedStart = (unDecodedStart+length)%bufferSize;
					
				}
			}
		    
		    dataLength -= length;
			if (dataLength < lastLength) rich = false;
			//System.out.println("read length: "+length+"  start: "+unDecodedStart+"  end: "+unDecodedEnd+"   dataLength: "+dataLength);
		    
		    
		} catch (Exception e) {
			// TODO: handle exception
			DFSClient.LOG.info(e.toString());
			
		} finally {	  
			preFetch.signal();
			lock.unlock();
		}
		
		readedPosition += length;
		return length==0?-1:length;
	    
	}
	 
	 class Monitor implements Runnable{

		@Override
		public void run() {
			// TODO Auto-generated method stub
			while(true){
				
				
				
				lock.lock();
				//System.out.println("lock");
				try {
					/*
					 * add a 'rich' condition because if length>dataLength>threshold then deadLock......
					 */
					while (dataLength>threshold && rich ) {
						preFetch.await();
					}
					
					//System.out.println("fetch");
					/*
					 * prefetech
					 */
					int preFetchSize = bufferSize-dataLength;
					byte[] temp = new byte[preFetchSize];
					int realLen = br.read(temp, 0, preFetchSize);
					if (realLen == -1){
						//may reach the EOF
						
						realLen = br.read(temp, 0, preFetchSize);
						if (realLen == -1) {	
							realLen = br.read(temp, 0, preFetchSize);
							if (realLen == -1) {	
								rich = true;
								run = false;
								break;
							}
						}
					}
					position += realLen;
					//System.out.println("realLen:  "+realLen);
					if(unDecodedEnd < unDecodedStart){
				    	/*end is right to start
				    	       end              start
				    	        |                 | 
				    	-----------------------------------
				    	|                                 |
				    	-----------------------------------
				    	*/
				    	System.arraycopy(temp, 0, buffer, unDecodedEnd, realLen);
				    	unDecodedEnd += realLen;
				    }
				    else {
						if(bufferSize-unDecodedEnd>=realLen){
							System.arraycopy(temp, 0, buffer, unDecodedEnd, realLen);
							unDecodedEnd += realLen;
						}
						else {
						/*
				    	    start                      end
				    	     |                          |
				    	 -----------------------------------
		                 |		    	                   |
				    	 ----------------------------------- 
				    	  
				    	 */
							System.arraycopy(temp, 0, buffer, unDecodedEnd, bufferSize-unDecodedEnd);
							System.arraycopy(temp, bufferSize-unDecodedEnd, buffer, 0, realLen-bufferSize+unDecodedEnd);
							unDecodedEnd = (unDecodedEnd+realLen)%bufferSize;
							
						}
					}
					
					dataLength += realLen;
					rich = true;
					//System.out.println("fetech realLen: "+realLen+" dataLength: "+dataLength);				
					
							
				} catch (Exception e) {
					// TODO: handle exception
					DFSClient.LOG.info("Exception in DataPool: "+e.toString());
					error = true;
				} finally {		
					enough.signal();
					lock.unlock();
				}
				
			}
				
		}
		 
	 }
	

}