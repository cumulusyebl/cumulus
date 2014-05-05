
package org.apache.hadoop.hdfs.server.monitor;
/**
 * author: xianyu
 * date: 2014-04-07
 * */

import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;

/**
 * to get the status of io, 
 * io_rate means how busy io is, that's to say, 
 * how often read/write happens in a specified time slot.
 * 
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * 
 * firstly, we get the directory where Servernode(NN/DN) stores data, 
 * then we find out which partition the directory mounted on, 
 * then find out the disk that the partition belongs to.
 * the find-out disk is the object that we will monitor.
 * 
 * the machine that Servernode(NN/DN) runs on may have more than one disk, 
 * so, as long as Servernode(NN/DN) store data on a disk, we will monitor it. 
 * we will create threads to do the actual job, one thread for one disk.
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * 
 * 
 * io_rate = io_rw_ticks / time_interval
 * io_rw_ticks cat be get from file '/sys/block/DISK-NAME/stat'.
 * */
public class ServernodeIOStator{
	private static final int IO_MONITOR_INTERVAL = 5000;//5s
	private static final String IO_STAT_MOUNTS_FILE = "/proc/mounts";
	private static final String IO_STAT_DISK_DIR = "/sys/block/";
	private static final String IO_STAT_DISKINFO_FILE = "stat";
	
//	private static final String DFS_DATANODE_DATA_DIR_KEY = "dfs.datanode.data.dir";
	private static final String HADOOP_TMP_DIR_KEY = "hadoop.tmp.dir";
	
	private int diskcount;
	private Worker[] workers;
	private Thread[]  threads;
	
	//detect: Servernode's data store on which disks.
	private String getDataDirs(Configuration conf, ServernodeRole role){
		String data_dirs = null;
		
		if(role == ServernodeRole.NAMENODE){
			data_dirs = conf.get(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY);
		}
		else if(role == ServernodeRole.DATANODE){
			data_dirs = conf.get(DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY);
		}
		
		if(data_dirs == null || data_dirs.length() == 0){
			data_dirs = conf.get(HADOOP_TMP_DIR_KEY);
			if(data_dirs .startsWith("file://"))
				data_dirs = data_dirs.substring("file://".length());
		}
		
		return data_dirs;
	}
	private ArrayList<String> getDisklist(Configuration conf, ServernodeRole role){
		String data_dirs = getDataDirs(conf, role);
		
		if(data_dirs == null || data_dirs.length() == 0)
			return null;
		
		String[] data_dir = data_dirs.trim().split("[\\s+,]");
		ArrayList<String> disklist = new ArrayList<String>(data_dir.length);
		
		for(String dd : data_dir){
			if(dd == null || dd.length() == 0)
				continue;
			if(!(new File(dd).isDirectory()))
				continue;
			
			//detect: the datanode-data-dir is mounted on which partition
			BufferedReader in = null;
			String mnt_info;
			String[] mnt_items;
			String[] partition = new String[2];
			try{
				partition[0] = partition[1] = null;
				in = new BufferedReader(new FileReader(IO_STAT_MOUNTS_FILE));
				while((mnt_info = in.readLine()) != null){
					if(!mnt_info.startsWith("/"))
						continue;
					mnt_items = mnt_info.split("\\s+");
					if(mnt_items.length < 2)
						continue;
					
					if(dd.startsWith(mnt_items[1])){
						if(partition[1] == null || 
								partition[1].length() <= mnt_items[1].length()){
							partition[0] = mnt_items[0];
							partition[1] = mnt_items[1];
						}
					}
				}
				
				//detect: the partition is on which disk
				if(partition[0] != null){
					//on Debian GNU/Linux 6.0.7 (squeeze), this is ok.
					//but, on Debian GNU/Linux 7.0 (wheezy), this causes problem.
					//we have fixed this bug.
//					String[] temp = partition[0].split("/");//delete
//					String temp2 = temp[temp.length - 1];//delete
					String partName = new File(partition[0])//add
											.getCanonicalFile().getName();
					File[] blocks = new File(IO_STAT_DISK_DIR).listFiles();
					for(File f : blocks){
						String disk = f.getName();
						if(partName.startsWith(disk)){
							if(!disklist.contains(disk))
								disklist.add(disk);
							break;
						}
					}
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
		}
		
		return disklist;
	}
	
	//detect: datanode's data store on which disks.
	//and create threads to monitor disk, one thread for one disk.
	public ServernodeIOStator(Configuration conf, ServernodeRole role){
		ArrayList<String> disklist = getDisklist(conf, role);
		
		if(disklist == null || disklist.size() == 0){
			workers = null;
			threads = null;
		}
		else{
			diskcount = disklist.size();
			workers = new Worker[diskcount];
			threads = new Thread[diskcount];
			
			for(int i = 0; i < diskcount; i++){
				workers[i] = new Worker(disklist.get(i));
				threads[i] = new Thread(workers[i]);
			}
		}
	}
	
	//start all threads
	public void activate(){
		if(threads != null){
			for(Thread t : threads){
				t.setDaemon(true);
				t.start();
			}
		}
	}
	
	public ServernodeIOStatus[] getStatus(){
		if(diskcount == 0)
			return null;
		
		ServernodeIOStatus[] ret = new ServernodeIOStatus[diskcount];
		for(int i = 0; i < diskcount; i++)
			ret[i] = new ServernodeIOStatus(workers[i].getStatus());
		
		return ret;
	}
	
	//the actual class that does the monitoring job
	private class Worker implements Runnable{
		private final String DISKINFO_FILE;
		
		private ServernodeIOStatus status;
		private long monitoring_count;
		private long init_io_rw_ticks;
		private long last_io_rw_ticks;
		private long cur_io_rw_ticks;
		
		public Worker(String diskname){
			DISKINFO_FILE = IO_STAT_DISK_DIR + diskname + 
					"/" + IO_STAT_DISKINFO_FILE;
			
			status = new ServernodeIOStatus(diskname);
			status.setMaxIORate(0);
			status.setMinIORate(100);
		}
		
		public ServernodeIOStatus getStatus(){
			return status;
		}
		
		public void run(){
			monitoring_count = 0;
			init_io_rw_ticks = getIORWTicks();
			last_io_rw_ticks = init_io_rw_ticks;
			
			while(true){
				try{
					Thread.sleep(IO_MONITOR_INTERVAL);
				}catch(InterruptedException e){
					e.printStackTrace();
				}
				
				cur_io_rw_ticks = getIORWTicks();
				monitoring_count ++;
				
				double cur_rate = (cur_io_rw_ticks - last_io_rw_ticks) * 1.0 / 
						IO_MONITOR_INTERVAL;
				status.setCurIORate(cur_rate);
				status.setAvgIORate((cur_io_rw_ticks - init_io_rw_ticks) * 1.0 / 
						(IO_MONITOR_INTERVAL * monitoring_count));
				if(cur_rate > status.getMaxIORate())
					status.setMaxIORate(cur_rate);
				if(cur_rate < status.getMinIORate())
					status.setMinIORate(cur_rate);
				
				last_io_rw_ticks = cur_io_rw_ticks;
				
//				System.out.println("[INFO by xianyu]" + status.toString());
			}
		}
		
		/**
		 * to get total_rw_ticks from file '/sys/block/DISK-NAME/stat'.
		 * 
		 * in the first line of file '/sys/block/DISK-NAME/stat', 
		 * the 10th item is total_rw_ticks.
		 * 
		 * the format of the file '/sys/block/DISK-NAME/stat': 
		 * $ cat /sys/block/sda/stat
		 *   rd_ios rd_merges rd_sec rd_ticks \
		 *   wr_ios wr_merges wr_sec wr_ticks \
		 *   ios_pgr tot_ticks rq_ticks
		 *   
		 * the 10th item above is total_rw_ticks, in millisecond.
		 * 
		 * note: actually, there's only one line in file '/sys/block/DISK-NAME/stat'.
		 * for details, see: https://www.kernel.org/doc/Documentation/iostats.txt
		 * */
		private long getIORWTicks(){
			BufferedReader in = null;
			
			try{
				in = new BufferedReader(new FileReader(DISKINFO_FILE));
				String str = in.readLine().trim();
				String[] items = str.split("\\s+");
				
				if(items.length < 11){
					throw new Exception(DISKINFO_FILE + "\'s output unrecognized. " + 
							"check: cat " + DISKINFO_FILE);
				}
				else{
					return Long.parseLong(items[items.length-2]);
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
			
			return 0;
		}
	}
}
