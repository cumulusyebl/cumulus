
package org.apache.hadoop.hdfs.server.monitor;
/**
 * author: xianyu
 * date: 2014-04-07
 * */

import java.io.FileReader;
import java.io.BufferedReader;

/**
 * to get the status of mem(memory), 
 * the mem's information is get from file '/proc/meminfo'.
 * 
 * mem_rate means how many memory is in-use, in percent.
 * mem_rate = 1 - (mem_free)/(mem_total)
 * mem_free and mem_total can be get from '/proc/meminfo'.
 * */
public class ServernodeMEMStator{
	private static final int MEM_MONITOR_INTERVAL = 5000;//5s
	private static final String MEM_STAT_FILE = "/proc/meminfo";
	
	private Worker worker;
	private Thread thread;
	
	public ServernodeMEMStator(){
		worker = new Worker();
		thread = new Thread(worker);
	}
	
	//start all threads
	public void activate(){
		thread.setDaemon(true);
		thread.start();
	}
	
	public ServernodeMEMStatus getStatus(){
		return new ServernodeMEMStatus(worker.getStatus());
	}
	
	//the actual class that does the monitoring job
	private class Worker implements Runnable{
		private ServernodeMEMStatus status;
		private long monitoring_count;
		
		public Worker(){
			status = new ServernodeMEMStatus();
			status.setMaxMEMRate(0);
			status.setMinMEMRate(100);
		}
		
		public ServernodeMEMStatus getStatus(){
			return status;
		}
		
		public void run(){
			monitoring_count = 0;
			
			while(true){
				try{
					Thread.sleep(MEM_MONITOR_INTERVAL);
				}catch(InterruptedException e){
					e.printStackTrace();
				}
				
				long[] cur_size = getMEMSize();
				double cur_rate = 1 - cur_size[0] * 1.0 / cur_size[1];

				monitoring_count ++;
				status.setCurMEMRate(cur_rate);
				status.setAvgMEMRate(status.getAvgMEMRate() + 
					(cur_rate - status.getAvgMEMRate()) * 1.0 / monitoring_count);
				if(cur_rate > status.getMaxMEMRate())
					status.setMaxMEMRate(cur_rate);
				if(cur_rate < status.getMinMEMRate())
					status.setMinMEMRate(cur_rate);
				
//				System.out.println("[INFO by xianyu]" + status.toString());
			}
		}
		
		/**
		 * to get mem_free_size and mem_total_size from file '/proc/meminfo'.
		 * 
		 * in the first line of file '/proc/meminfo', the 2th item is mem_total_size, 
		 * in the next 3 lines of file '/proc/meminfo', the sum of 2th item is mem_free_size.
		 * 
		 * the format of the file '/proc/meminfo': 
		 * $ cat /proc/meminfo
		 *   MemTotal:	size1 kB
		 *   MemFree:	size2 kB
		 *   Buffers:	size3 kB
		 *   Cached:	size4 kB
		 *   ... ...//others lines
		 *   
		 * note: Due to linux's memory management mechanism, 
		 *       the Buffers and Cached should be treated as free memory.
		 * so: mem_free_size = MemFree + Buffers + Cached;
		 *     mem_total_size = MemTotal;
		 * 
		 * @returns return an array, length is 2, 
		 *          the first element is mem_free_size, 
		 *          the second element is mem_total_size.
		 * */
		private long[] getMEMSize(){
			long[] ret = new long[2];
			BufferedReader in = null;
			
			try{
				in = new BufferedReader(new FileReader(MEM_STAT_FILE));
				
				//get mem total size
				String str = in.readLine();
				String[] items = str.trim().split("\\s+");
				if(items == null || items.length != 3){
					throw new Exception(MEM_STAT_FILE + "\'s output unrecognized. " + 
							"check: cat " + MEM_STAT_FILE);
				}
				else{
					ret[1] = Long.parseLong(items[1]);
				}
				
				//get mem free size
				ret[0] = 0;
				for(int cnt = 0; cnt < 3; cnt++){
					str = in.readLine();
					items = str.trim().split("\\s+");
					if(items == null || items.length != 3){
						throw new Exception(MEM_STAT_FILE + "\'s output unrecognized. " + 
								"check: cat " + MEM_STAT_FILE);
					}
					else{
						ret[0] += Long.parseLong(items[1]);
					}
				}
			}catch(Exception e){
				ret[0] = 0;//avoid to divide zero, when mem_rate is 0, sth must be wrong.
				ret[1] = 1;
				e.printStackTrace();
			}finally{
				if(in != null){
					try{
						in.close();
					}catch(Exception e){
					}
				}
			}
			
			return ret;
		}
	}
}
