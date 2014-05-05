
package org.apache.hadoop.hdfs.server.monitor;
/**
 * author: xianyu
 * date: 2014-04-07
 * */

import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;
import java.util.ArrayList;

/**
 * to get the status of net(network), 
 * net_rate means the network speed, in byte per second.
 * rx : download
 * tx : upload 
 * 
 * we can get the net card's name from this dir: 
 * 		/sys/class/net/
 * the sub-dirs of '/sys/class/net' are those net card's names.
 * we will detect which net card(s) is/are in-use. 
 * there may be more than one card that is in-use, we will monitor everyone.
 * we will create threads to do the actual job, one thread for one net card.
 * 
 * we get the certain card's information from these file: 
 * 		/sys/class/net/CARD-NAME/operstate  -> up | down(work or not)
 * 		                         speed  -> bandwidth(max speed theoretically)
 * 		                         duplex  -> full | half
 * 		                         statistics/rx_bytes  -> receive bytes
 * 		                         statistics/tx_bytes  -> transform bytes 
 * 
 * we use the netword speed to represent the status of network.
 * 
 * receiving speed = (cur_rx_bytes - last_rx_bytes) / time_interval
 * sending speed = (cur_tx_bytes - last_rx_bytes) / time_interval
 * */
public class ServernodeNETStator{
	private static final int NET_MONITOR_INTERVAL = 5000;//5s
	private static final String NET_STAT_DIR = "/sys/class/net/";
	private static final String NET_STAT_OPERSTATE_FILE = "operstate";
	private static final String NET_STAT_BANDWIDTH_FILE = "speed";
	private static final String NET_STAT_DUPLEX_FILE = "duplex";
	private static final String NET_STAT_RX_FILE = "statistics/rx_bytes";
	private static final String NET_STAT_TX_FILE = "statistics/tx_bytes";
	
	private int cardcount = 0;
	private Worker[] workers = null;
	private Thread[] threads = null;
	
	//detect which net card are in-use(that is, operastate is up), 
	//returns a list of in-use net card.
	private ArrayList<String> getCardlist(){
		File dir = new File(NET_STAT_DIR);
		File[] filelist = dir.listFiles();
		
		//there's no net card, or NET_STAT_DIR not exist.
		if(filelist == null || filelist.length == 0)
			return null;
		
		ArrayList<String> cardlist = new ArrayList<String>(filelist.length);
		for(File f : filelist){
			if(!f.isDirectory())
				continue;
			if(f.getName().equalsIgnoreCase("lo"))
				continue;//ignore loopback
			if(f.getName().startsWith("wlan"))
				continue;//ignore wireless
			
			BufferedReader in = null;
			try{
				in = new BufferedReader(new FileReader(
						NET_STAT_DIR + f.getName() + "/" + NET_STAT_OPERSTATE_FILE));
				if(in.readLine().trim().equalsIgnoreCase("up")){
					cardlist.add(NET_STAT_DIR + f.getName() + "/");
				}
			}catch(Exception e){
				continue;
			}finally{
				if(in != null){
					try{
					in.close();
					}catch(Exception e){						
					}
				}
			}
		}
		
		return cardlist;
	}
	
	//detect which net card are in-use(that is, operastate is up), 
	//and create threads to monitor net card, one thread for one net card.
	public ServernodeNETStator(){
		ArrayList<String> cardlist = getCardlist();
		
		if(cardlist.size() > 0){
			cardcount = cardlist.size();
			workers = new Worker[cardcount];
			threads = new Thread[cardcount];
			
			for(int i = 0; i < cardcount; i++){
				workers[i] = new Worker(cardlist.get(i));
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
	
	public ServernodeNETStatus[] getStatus(){
		if(cardcount == 0)
			return null;
		
		ServernodeNETStatus[] ret = new ServernodeNETStatus[cardcount];
		for(int i = 0; i < cardcount; i++){
			ret[i] = new ServernodeNETStatus(workers[i].getStatus());
		}
		
		return ret;
	}
	
	//the actual class that does the monitoring job
	private class Worker implements Runnable{
		private final String BANDWIDTH_FILE;
		private final String DUPLEX_FILE;
		private final String RX_FILE;
		private final String TX_FILE;
		
		private ServernodeNETStatus status;
		private long monitoring_count;
		private long init_rx_bytes, init_tx_bytes;
		private long last_rx_bytes, last_tx_bytes;
		private long cur_rx_bytes, cur_tx_bytes;
		
		public Worker(String cardname){
			BANDWIDTH_FILE = cardname + NET_STAT_BANDWIDTH_FILE;
			DUPLEX_FILE = cardname + NET_STAT_DUPLEX_FILE;
			RX_FILE = cardname + NET_STAT_RX_FILE;
			TX_FILE = cardname + NET_STAT_TX_FILE;
			
			status = new ServernodeNETStatus(cardname);
			status.setBandwidth(getNETBandwidth());
			status.setDuplex(getNETDuplex());
			status.setMinNETRxRate(status.getBandwidth()*1000*1000);
			status.setMinNETTxRate(status.getBandwidth()*1000*1000);
		}
		
		public ServernodeNETStatus getStatus(){
			return status;
		}
		
		public void run(){
			monitoring_count = 0;
			init_rx_bytes = getNETxBytes(RX_FILE);
			init_tx_bytes = getNETxBytes(TX_FILE);
			last_rx_bytes = init_rx_bytes;
			last_tx_bytes = init_tx_bytes;
			
			while(true){
				try{
					Thread.sleep(NET_MONITOR_INTERVAL);
				}catch(InterruptedException e){
					e.printStackTrace();
				}
				
				cur_rx_bytes = getNETxBytes(RX_FILE);
				cur_tx_bytes = getNETxBytes(TX_FILE);
				monitoring_count ++;
				
				double cur_rx_rate = 
					(cur_rx_bytes - last_rx_bytes) * 1.0 / (NET_MONITOR_INTERVAL / 1000);
				double cur_tx_rate = 
					(cur_tx_bytes - last_tx_bytes) * 1.0 / (NET_MONITOR_INTERVAL / 1000);
				
				status.setCurNETRxRate(cur_rx_rate);//cur rx
				status.setCurNETTxRate(cur_tx_rate);//cur tx
				status.setAvgNETRxRate(
					(cur_rx_bytes - init_rx_bytes) * 1.0 / 
						(NET_MONITOR_INTERVAL / 1000 * monitoring_count));//avg rx
				status.setAvgNETTxRate(
					(cur_tx_bytes - init_tx_bytes) * 1.0 / 
						(NET_MONITOR_INTERVAL / 1000 * monitoring_count));//avg tx
				if(cur_rx_rate > status.getMaxNETRxRate())//max rx
					status.setMaxNETRxRate(cur_rx_rate);
				if(cur_tx_rate > status.getMaxNETTxRate())//max tx
					status.setMaxNETTxRate(cur_tx_rate);
				if(cur_rx_rate < status.getMinNETRxRate())//min rx
					status.setMinNETRxRate(cur_rx_rate);
				if(cur_tx_rate < status.getMinNETTxRate())//min tx
					status.setMinNETTxRate(cur_tx_rate);
				
				last_rx_bytes = cur_rx_bytes;
				last_tx_bytes = cur_tx_bytes;
				
//				System.out.println("[INFO by xianyu]" + status.toString());
			}
		}
		
		//read bandwidth of a net card, 
		//from file /sys/class/net/CARD-NAME/speed.
		private int getNETBandwidth(){
			BufferedReader in = null;
			
			try{
				in = new BufferedReader(new FileReader(BANDWIDTH_FILE));
				String str = in.readLine().trim();
				if(str != null)
					return Integer.parseInt(str);
				else
					return -1;
			}catch(Exception e){
			}finally{
				if(in != null){
					try{
						in.close();
					}catch(Exception e){
					}
				}
			}
			
			return -1;
		}
		
		//read duplex of a net card, 
		//from file /sys/class/net/CARD-NAME/duplex.
		private String getNETDuplex(){
			BufferedReader in = null;
			
			try{
				in = new BufferedReader(new FileReader(DUPLEX_FILE));
				String str = in.readLine().trim();
				if(str.equalsIgnoreCase("full") || str.equalsIgnoreCase("half"))
					return str;
				else
					return "unknown";
			}catch(Exception e){
			}finally{
				if(in != null){
					try{
						in.close();
					}catch(Exception e){
					}
				}
			}
			
			return "unknown";
		}
		
		//read rx_bytes/tx_bytes of a net card, 
		//from file:
		//	rx_bytes: /sys/class/net/CARD-NAME/statistics/rx_bytes
		//	tx_bytes: /sys/class/net/CARD-NAME/statistics/tx_bytes
		private long getNETxBytes(String filename){
			BufferedReader in = null;
			
			try{
				in = new BufferedReader(new FileReader(filename));
				String str = in.readLine().trim();
				if(str != null)
					return Long.parseLong(str);
				else
					return 0;
			}catch(Exception e){
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
