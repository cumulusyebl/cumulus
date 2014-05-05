
package org.apache.hadoop.hdfs.server.monitor;
/**
 * author: xianyu
 * date: 2014-04-07
 * */

import java.io.FileReader;
import java.io.BufferedReader;

/**
 * to get the status of cpu, 
 * the cpu's information is get from file '/proc/stat'.
 * 
 * cpu_rate means how often cpu is in-use, in percent.
 * cpu_rate = 1 - (cpu_idle_time)/(cpu_total_time)
 * cpu_idle_time and cpu_total_time can be get from file '/proc/stat'.
 * */
public class ServernodeCPUStator{
	private static final int CPU_MONITOR_INTERVAL = 5000;//5s
	private static final String CPU_STAT_FILE = "/proc/stat";
	
	private Worker worker;
	private Thread thread;
	
	public ServernodeCPUStator(){
		worker = new Worker();
		thread = new Thread(worker);
	}
	
	//start all threads
	public void activate(){
		thread.setDaemon(true);
		thread.start();
	}
	
	public ServernodeCPUStatus getStatus(){
		return new ServernodeCPUStatus(worker.getStatus());
	}
	
	//the actual class that does the monitoring job
	private class Worker implements Runnable{
		private ServernodeCPUStatus status;
		private long init_cpu_idle_time, init_cpu_total_time;
		private long last_cpu_idle_time, last_cpu_total_time;
		private long cur_cpu_idle_time, cur_cpu_total_time;
		
		public Worker(){
			status = new ServernodeCPUStatus();
			status.setMaxCPURate(0);
			status.setMinCPURate(100);
		}
		
		public ServernodeCPUStatus getStatus(){
			return status;
		}
		
		public void run(){
			long[] temp = getCPUTime();
			init_cpu_idle_time = temp[0];
			init_cpu_total_time = temp[1];
			last_cpu_idle_time = temp[0];
			last_cpu_total_time = temp[1];
			
			while(true){
				try{
					Thread.sleep(CPU_MONITOR_INTERVAL);
				}catch(InterruptedException e){
					e.printStackTrace();
				}
				
				temp = getCPUTime();
				cur_cpu_idle_time = temp[0];
				cur_cpu_total_time = temp[1];
				
				double cur_rate = 1 - 
					(cur_cpu_idle_time - last_cpu_idle_time) * 1.0 / 
						(cur_cpu_total_time - last_cpu_total_time);
				status.setCurCPURate(cur_rate);
				status.setAvgCPURate(1 - 
					(cur_cpu_idle_time - init_cpu_idle_time) * 1.0 / 
						(cur_cpu_total_time - init_cpu_total_time));
				if(cur_rate > status.getMaxCPURate())
					status.setMaxCPURate(cur_rate);
				if(cur_rate < status.getMinCPURate())
					status.setMinCPURate(cur_rate);
				
				last_cpu_idle_time = cur_cpu_idle_time;
				last_cpu_total_time = cur_cpu_total_time;
				
//				System.out.println("[INFO by xianyu]" + status.toString());
			}
		}
		
		/**
		 * to get cpu_idle_time and cpu_total_time from file '/proc/stat'.
		 * 
		 * in the first line of file '/proc/stat', 
		 * the 5th item is cpu_idle_time, 
		 * the sum of all items except the first one is cpu_total_time.
		 * 
		 * the format of the file '/proc/stat': 
		 * $ cat /proc/stat
		 *   cpu time1 time2 time3 time4(idle) time5 time6 time7 time8 time9 time10
		 *   ... ... //other lines
		 *   
		 * @returns return an array, length is 2, 
		 *          the first element is cpu_idle_time, 
		 *          the second element is cpu_total_time.
		 * */
		private long[] getCPUTime(){
			long[] ret = new long[2];
			BufferedReader in = null;
			
			try{
				in = new BufferedReader(new FileReader(CPU_STAT_FILE));
				String str = in.readLine();
				String[] items = str.trim().split("\\s+");
				
				if(items == null || items.length < 5){
					throw new Exception(CPU_STAT_FILE + "\'s output unrecognized. " + 
							"check: cat " + CPU_STAT_FILE);
				}
				
				//get cpu idle time
				ret[0] = Long.parseLong(items[4]);
				
				//get cpu total time
				ret[1] = 0;
				for(int idx = 1; idx < items.length; idx++){
					ret[1] += Long.parseLong(items[idx]);
				}
			}catch(Exception e){
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
