
package org.apache.hadoop.hdfs.server.namenode;
/**
 * author: xianyu
 * date: 2014-04-08
 * */

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.security.PrivilegedExceptionAction;
import java.util.Date;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.hdfs.protocol.CodingMatrix;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.FSConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.FSConstants.UpgradeAction;
import org.apache.hadoop.hdfs.server.common.JspHelper;
import org.apache.hadoop.hdfs.server.common.UpgradeStatusReport;
import org.apache.hadoop.hdfs.server.monitor.ServernodeStator;
import org.apache.hadoop.hdfs.server.namenode.DfsServlet;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.VersionInfo;
import org.apache.hadoop.hdfs.server.monitor.*;
import org.json.*;


public class MonitorServlet extends DfsServlet{
	/** For java.io.Serializable */
	private static final long serialVersionUID = 1L;
	/*
	private static final String PATH_PREFIX = "/user/" + 
				System.getProperty("hadoop.id.str") + "/";*/
	
	/*
	private static String repairPath(String path){
		if(path == null)
			return null;
		if(!path.startsWith("/"))
			return PATH_PREFIX + path;
		if(path.startsWith(PATH_PREFIX))
			return path;
		return PATH_PREFIX + path.substring(1);
	}
	*/
	
	/**
	* Service a GET request as described below.
	* Request:
	* 	GET http://<host>:<port>/monitor?class=...&key=...&... HTTP/1.1
	* */
	public void doGet(final HttpServletRequest request, final HttpServletResponse response)
		throws IOException {
		
		final ServletContext context = getServletContext();
	    final Configuration conf = 
	    		(Configuration) context.getAttribute(JspHelper.CURRENT_CONF);
		final UserGroupInformation ugi = getUGI(request, conf);
		
		System.out.println("[" + new Date().toString() + "]" + 
			request.getRequestURL() + 
				(request.getQueryString() == null ? "" : ("?"+request.getQueryString())));
		
		try{
			ugi.doAs(new PrivilegedExceptionAction<Void>(){
				public Void run() throws Exception {
					doAll(request, response);
					return null;
				}
			});
		}catch(InterruptedException e){
			throw new IOException(e);
		}
	}
	
	private void doBadRequest(
			HttpServletRequest request, HttpServletResponse response, String msg)
			throws Exception{
		JSONObject jsonobj = new JSONObject();
		jsonobj.put("errmsg", msg);
		
		response.setContentType("application/json");
		PrintWriter out = response.getWriter();
		out.print(jsonobj);
		out.flush();
	}
	
	private void doAll(HttpServletRequest request, HttpServletResponse response)
		throws Exception{
		String whichclass = StringEscapeUtils.unescapeHtml(
				request.getParameter("class"));
		
		if(whichclass == null || whichclass.length() == 0){
			doBadRequest(request, response, "need class");
		}
		else if(whichclass.equalsIgnoreCase("namenode")){
			getNamenodeInfo(request, response);
		}
		else if(whichclass.equalsIgnoreCase("datanode")){
			getDatanodeInfo(request, response);
		}
		else if(whichclass.equalsIgnoreCase("file")){
			//for directory and non-directory(that is, normal file)
			getFileInfo(request, response);
		}
		else if(whichclass.equalsIgnoreCase("block")){
			getBlockInfo(request, response);
		}
		else if(whichclass.equalsIgnoreCase("log")){
			getLogInfo(request, response);
		}
		else if(whichclass.equalsIgnoreCase("conf")){
			getConfigurationInfo(request, response);
		}
		else{
			doBadRequest(request, response, "class invalid");
		}
	}

	
	/** * * * * * * * * * * * * * * * * * * * * * * * * * *
	request for namenode:
		http://host:port/monitor?class=namenode&key=version
		http://host:port/monitor?class=namenode&key=storage
		http://host:port/monitor?class=namenode&key=status
		http://host:port/monitor?class=namenode&key=datanodes
	* * * * * * * * * * * * * * * * * * * * * * * * * * **/
	private void getNamenodeInfo(
			HttpServletRequest request, HttpServletResponse response)
			throws Exception{

		NameNode nn = (NameNode)getServletContext().getAttribute("name.node");
		FSNamesystem fsn = nn.getNamesystem();
		PrintWriter out = response.getWriter();
		JSONObject jsonobj = new JSONObject();
		
		String key = StringEscapeUtils.unescapeHtml(
				request.getParameter("key"));
		if(key == null || key.length() == 0){
			/* sample
				{
					"errmsg":"need class"
				}
			 */
			doBadRequest(request, response, "need key(class: namenode)");
			return ;
		}
		else if(key.equalsIgnoreCase("version")){
			// request: 
			//		http://host:port/monitor?class=namenode&key=version
			// response: 
			//		"name-info":
			//			"host-ip":STRING
			//			"host-name":STRING
			//			"host-port":INT
			//		"role":STRING
			//		"start time":STRING
			//		"version-info":
			//			"version":STRING
			//			"revision":STRING
			//		"compile-info":
			//			"author":STRING
			//			"branch":STRING
			//			"compile-time":STRING
			//		"upgrade-info":STRING
			/* output sample
				{
					"name-info":
						{
							"host-ip":"114.212.81.5",
							"host-name":"cumulus",
							"host-port":9000
						},
					"role":"NameNode",
					"start-time":"Fri Apr 11 00:50:28 CST 2014",
					"version-info":
						{
							"revision":"Unknown",
							"version":"0.22.1-SNAPSHOT"
						},
					"compile-info":
						{
							"author":"xianyu",
							"branch":"Unknown",
							"compile-time":"Mon Mar 10 12:44:50 CST 2014"
						},
					"upgrade-info":"There are no upgrades in progress."
				}
			*/
			String[] ss = nn.getNameNodeAddress().getAddress().toString().split("/");
			jsonobj.put("name_info", 
						new JSONObject()
							.put("host_name", nn.getNameNodeAddress().getHostName())
							.put("host_ip", ss[ss.length-1])
							.put("host_port", nn.getNameNodeAddress().getPort()));
			jsonobj.put("role", 
						nn.getRole().toString());
			jsonobj.put("start_time", 
						fsn.getStartTime());
			jsonobj.put("version_info", 
						new JSONObject()
							.put("version", VersionInfo.getVersion())
							.put("revision", VersionInfo.getRevision()));
			jsonobj.put("compile_info", 
						new JSONObject()
							.put("compile_time", VersionInfo.getDate())
							.put("author", VersionInfo.getUser())
							.put("branch", VersionInfo.getBranch()));
			String statusText = "";
			try{
				UpgradeStatusReport status = 
						fsn.distributedUpgradeProgress(UpgradeAction.GET_STATUS);
				statusText = ((status == null) ? 
								"There are no upgrades in progress." : 
								status.getStatusText(false));
			}catch(IOException e){
				statusText = "Upgrade status unknown.";
			}
			jsonobj.put("upgrade_info", 
						statusText);
			
			//response, json format
			response.setContentType("application/json");
			out.print(jsonobj);
			out.flush();
			return ;
		}
		else if(key.equalsIgnoreCase("storage")){
			// request:
			//		http://host:port/monitor?class=namenode&key=storage
			// response: 
			//		"capacity-total":LONG,
			//		"capacity-dfs-used":LONG,
			//		"capacity-non-dfs-used":LONG,
			//		"capacity-remaining":LONG,
			//		"total-load":INT,
			//		"num-of-blocks":LONG,
			//		"num-of-files":LONG,
			//		"num-of-missing-blocks":LONG,
			//		"num-of-corrupt-replica-blocks":LONG,
			//		"num-of-under-replicated-blocks":LONG
			/* output sample
				{
					"capacity-total":12596362813440,
					"capacity-dfs-used":127627264,
					"capacity-non-dfs-used":719509483520,
					"capacity-remaining":11876725702656,
					"total-load":7,
					"num-of-blocks":15,
					"num-of-files":9,
					"num-of-missing-blocks":0,
					"num-of-corrupt-replica-blocks":0,
					"num-of-under-replicated-blocks":0
				}
			 */
			
			jsonobj.put("capacity_total", 
						fsn.getCapacityTotal());
			jsonobj.put("capacity_dfs_used", 
						fsn.getCapacityUsed());
			jsonobj.put("capacity_non_dfs_used", 
						fsn.getCapacityUsedNonDFS());
			jsonobj.put("capacity_remaining", 
						fsn.getCapacityRemaining());
			jsonobj.put("num_of_under_replicated_blocks", 
						fsn.getUnderReplicatedBlocks());
			jsonobj.put("num_of_corrupt_replica_blocks", 
						fsn.getCorruptReplicaBlocks());
			jsonobj.put("num_of_missing_blocks", 
						fsn.getMissingBlocksCount());
			jsonobj.put("total_load", 
						fsn.getTotalLoad());
			jsonobj.put("num_of_files", 
						fsn.getTotalFiles());
			jsonobj.put("num_of_blocks", 
						fsn.getTotalBlocks());
			
			//response, json format
			response.setContentType("application/json");
			out.print(jsonobj);
			out.flush();
			return ;
		}
		else if(key.equalsIgnoreCase("status")){
			// request:
			//		http://host:port/monitor?class=namenode&key=status
			// response: 
			//		"cpu-status": see below, for details
			//		"mem-status":
			//		"net-status":
			//		"io-status":
			/* output sample
				{
					"io-status":
						[
							{
								"diskname":"sda",
								"cur-rate":8.0E-4,
								"avg-rate":0.0026052785923753666,
								"max-rate":0.1768,
								"min-rate":0
							}
						],
					"net-status":
						[
							{
								"cardname":"eth0",
								"duplex":"full",
								"bandwidth":1000,
								"cur-tx-rate":342.4,
								"cur-rx-rate":2010.4,
								"avg-tx-rate":39643.337536656894,
								"avg-rx-rate":3550.5548387096774,
								"max-rx-rate":203278.2,
								"max-tx-rate":1.18730026E7,
								"min-rx-rate":98.4,
								"min-tx-rate":13.2
							}
						],
					"mem-status":
						{
							"cur-rate":0.392769409689931,
							"avg-rate":0.38441373951630564,
							"max-rate":0.3990643899754347,
							"min-rate":0.3652153128307928
						},
					"cpu-status":
						{
							"cur-rate":1,
							"avg-rate":0.9996408101271019,
							"max-rate":1,
							"min-rate":0.9085457271364318
						}
				}
			 */
			
			ServernodeStator nnStator = nn.getServernodeStator();
			ServernodeCPUStatus cpuStatus = nnStator.getCPUStatus();
			ServernodeMEMStatus memStatus = nnStator.getMEMStatus();
			ServernodeNETStatus[] netStatus = nnStator.getNETStatus();
			ServernodeIOStatus[] ioStatus = nnStator.getIOStatus();
			
			jsonobj.put("cpu_status", 
						new JSONObject()
							.put("cur_rate", cpuStatus.getCurCPURate())
							.put("avg_rate", cpuStatus.getAvgCPURate())
							.put("max_rate", cpuStatus.getMaxCPURate())
							.put("min_rate", cpuStatus.getMinCPURate()));
			jsonobj.put("mem_status", 
						new JSONObject()
							.put("cur_rate", memStatus.getCurMEMRate())
							.put("avg_rate", memStatus.getAvgMEMRate())
							.put("max_rate", memStatus.getMaxMEMRate())
							.put("min_rate", memStatus.getMinMEMRate()));
			JSONArray jsonNetArray = new JSONArray();
			for(int i = 0; i < netStatus.length; i++){
				jsonNetArray.put(
					new JSONObject()
						.put("cardname", netStatus[i].getCardname())
						.put("bandwidth", netStatus[i].getBandwidth())
						.put("duplex", netStatus[i].getDuplex())
						.put("cur_rx_rate", netStatus[i].getCurNETRxRate())
						.put("cur_tx_rate", netStatus[i].getCurNETTxRate())
						.put("avg_rx_rate", netStatus[i].getAvgNETRxRate())
						.put("avg_tx_rate", netStatus[i].getAvgNETTxRate())
						.put("max_rx_rate", netStatus[i].getMaxNETRxRate())
						.put("max_tx_rate", netStatus[i].getMaxNETTxRate())
						.put("min_rx_rate", netStatus[i].getMinNETRxRate())
						.put("min_tx_rate", netStatus[i].getMinNETTxRate()));
			}
			jsonobj.put("net_status", 
						jsonNetArray);
			
			JSONArray jsonIOArray = new JSONArray();
			for(int j = 0; j < ioStatus.length; j++){
				jsonIOArray.put(
					new JSONObject()
						.put("diskname", ioStatus[j].getDiskname())
						.put("cur_rate", ioStatus[j].getCurIORate())
						.put("avg_rate", ioStatus[j].getAvgIORate())
						.put("max_rate", ioStatus[j].getMaxIORate())
						.put("min_rate", ioStatus[j].getMinIORate()));
			}
			jsonobj.put("io_status", 
						jsonIOArray);
			
			//response, json format
			response.setContentType("application/json");
			out.print(jsonobj);
			out.flush();
			return ;
		}
		else if(key.equalsIgnoreCase("datanodes")){
			// request: 
			//		http://host:port/monitor?class=namenode&key=datanodes
			// response: 
			//		"num-of-datanode":INT,
			//		"num-of-live-datanode":INT,
			//		"num-of-dead-datanode":INT,
			//		"num-of-decommissioning-datanode":INT,
			//		"datanode-storage-id":STRING[]
			/* output sample
				{
					"num-of-datanode":7,
					"num-of-live-datanode":7,
					"num-of-dead-datanode":0,
					"num-of-decommissioning-datanode":0,
					"datanode-storage-id":
						[
							"DS-1034188672-127.0.1.1-50010-1397148710275",
							"DS-1219633777-114.212.83.113-50010-1397148419175",
							"DS-1513686327-114.212.82.19-50010-1397148649936",
							"DS-170182321-114.212.82.2-50010-1397177982235",
							"DS-2127572503-114.212.83.185-50010-1397148650021",
							"DS-332527351-114.212.82.13-50010-1397148664798",
							"DS-730681320-114.212.82.16-50010-1397148666970"
						]
				}
			 */
			/*
			jsonobj.put("datanode_storage_id", 
					fsn.datanodeMap.keySet());
			jsonobj.put("num_of_datanode", 
						fsn.getNumberOfDatanodes(DatanodeReportType.ALL));
			jsonobj.put("num_of_live_datanode", 
						fsn.getNumLiveDataNodes());
			jsonobj.put("num_of_dead_datanode", 
						fsn.getNumDeadDataNodes());
			jsonobj.put("num_of_decommissioning_datanode", 
						fsn.getDecommissioningNodes().size());
			*/
			/*
			 {
			  	 "num-of-datanode":INT,
			  	 "num-of-live-datanode":INT,
			  	 "num-of-dead-datanode":INT,
			  	 "num-of-decommissioning-datanode":INT,
			  	 "live-datanode-storage-id":STRING[], 
			  	 "dead-datanode-storage-id":STRING[], 
			  	 "decommissioning-datanode-storage-id":STRING[]
			}
			 */
			jsonobj.put("num_of_datanode", 
						fsn.getNumberOfDatanodes(DatanodeReportType.ALL));
			jsonobj.put("num_of_live_datanode", 
						fsn.getNumLiveDataNodes());
			jsonobj.put("num_of_dead_datanode", 
						fsn.getNumDeadDataNodes());
			jsonobj.put("num_of_decommissioning_datanode", 
						fsn.getDecommissioningNodes().size());
			jsonobj.put("live_datanode_storage_id", 
						fsn.getLiveDataNodesStorageID());
			jsonobj.put("dead_datanode_storage_id", 
						fsn.getDeadDataNodesStorageID());
			jsonobj.put("decommissioning_datanode_storage_id", 
						fsn.getDecommissioningNodesStorageID());
			
			
			//response, json format
			response.setContentType("application/json");
			out.print(jsonobj);
			out.flush();
			return ;
		}
		else{
			/* sample
				{
					"errmsg":"key invalid(class: namenode)"
				}
			 */
			doBadRequest(request, response, "key invalid(class: namenode)");
			return ;
		}
	}
	
	
	/** * * * * * * * * * * * * * * * * * * * * * * * * * *
	request for datanode:
		http://host:port/monitor?class=datanode&storageID=...&key=version
		http://host:port/monitor?class=datanode&storageID=...&key=storage
		http://host:port/monitor?class=datanode&storageID=...&key=situation
		http://host:port/monitor?class=datanode&storageID=...&key=status
	* * * * * * * * * * * * * * * * * * * * * * * * * * **/
	private void getDatanodeInfo(
			HttpServletRequest request, HttpServletResponse response)
			throws Exception{
		NameNode nn = (NameNode)getServletContext().getAttribute("name.node");
		FSNamesystem fsn = nn.getNamesystem();
		PrintWriter out = response.getWriter();
		JSONObject jsonobj = new JSONObject();
		
		String storageID = StringEscapeUtils.unescapeHtml(
				request.getParameter("storageID"));
		if(storageID == null || storageID.length() == 0){
			doBadRequest(request, response, "need storageID(class: datanode)");
			return ;
		}
		DatanodeDescriptor dndsp = fsn.getDatanode(storageID);
		if(dndsp == null){
			doBadRequest(request, response, "storageID(" + 
						storageID + ") invalid(class: datanode)");
			return ;
		}
		
		String key = StringEscapeUtils.unescapeHtml(
				request.getParameter("key"));
		if(key == null || key.length() == 0){
			doBadRequest(request, response, "need key(class: datanode)");
			return ;
		}
		else if(key.equalsIgnoreCase("version")){
		// request:
		//		http://host:port/monitor?class=datanode&storageID=...&key=version
		// response: 
		//		"name-info":
		//			"host-ip":STRING
		//			"host-name":STRING
		//			"host-port":INT
		//		"storage-id":STRING
		//		"network":
		//			"location":STRING
		//			"level":INT
		//		"register-time":LONG
		/* output sample
		{
			"register-time":1397148668252,
			"storage-id":"DS-1219633777-114.212.83.113-50010-1397148419175",
			"network":
				{
					"level":2,
					"location":"/default-rack"
				},
			"name-info":
				{
					"host-ip":"114.212.83.113",
					"host-name":"haibus",
					"host-port":50010
				}
		}
		*/
			jsonobj.put("name_info", 
					new JSONObject()
						.put("host_name", dndsp.getHostName())
						.put("host_ip", dndsp.getHost())
						.put("host_port", dndsp.getPort()));
			jsonobj.put("storage_id", 
						dndsp.getStorageID());
			jsonobj.put("network", 
						new JSONObject()
							.put("location", dndsp.getNetworkLocation())
							.put("level", dndsp.getLevel()));
			jsonobj.put("register_time", 
						dndsp.getRegisterTime());
			
			//response, json format
			response.setContentType("application/json");
			out.print(jsonobj);
			out.flush();
			return ;
		}
		else if(key.equalsIgnoreCase("storage")){
		// request: 
		//		http://host:port/monitor?class=datanode&storageID=...&key=storage
		// response: 
		//		"capacity-total":LONG // in byte
		//		"capacity-dfs-used":LONG // in byte
		//		"capacity-non-dfs-used":LONG // in byte
		//		"capacity-remaining":LONG // in byte
		//		"num-of-failed-volumes":INT
		//		"num-of-blocks":INT
		//		"num-of-replicate-blocks":INT
		//		"num-of-invalidate-blocks":INT
		//		"num-of-recover-blocks":INT
		/* output sample
			{
				"capacity-total":2894675496960,
				"capacity-dfs-used":14200832,
				"capacity-non-dfs-used":153399390208,
				"capacity-remaining":2741261905920,
				"num-of-blocks":2,
				"num-of-recover-blocks":0,
				"num-of-replicate-blocks":0,
				"num-of-failed-volumes":0,
				"num-of-invalidate-blocks":0
			}
		*/
			jsonobj.put("capacity_total", 
						dndsp.getCapacity());
			jsonobj.put("capacity_dfs_used", 
						dndsp.getDfsUsed());
			jsonobj.put("capacity_non_dfs_used", 
						dndsp.getNonDfsUsed());
			jsonobj.put("capacity_remaining", 
						dndsp.getRemaining());
			jsonobj.put("num_of_failed_volumes", 
						dndsp.getVolumeFailures());
			jsonobj.put("num_of_blocks", 
						dndsp.numBlocks());
			jsonobj.put("num_of_replicate_blocks", 
						dndsp.getNumberOfBlocksToBeReplicated());
			jsonobj.put("num_of_invalidate_blocks", 
						dndsp.getNumberOfBlocksToBeInvalidated());
			jsonobj.put("num_of_recover_blocks", 
						dndsp.getNumberOfBlocksToBeRecoved());
			
			//response, json format
			response.setContentType("application/json");
			out.print(jsonobj);
			out.flush();
			return ;
		}
		else if(key.equalsIgnoreCase("situation")){
		// request:
		//		http://host:port/monitor?class=datanode&storageID=...&key=situation
		// response: 
		//		"last contact":LONG
		//		"admin state":STRING
		//		"num-of-active-connections":INT
		/* output sample
			{
				"admin-state":"Live",
				"num-of-active-connections":1,
				"last-contact":1397150054816
			}
		*/
			jsonobj.put("last_contact", 
						dndsp.getLastUpdate());
			jsonobj.put("admin_state", 
						(dndsp.isDecommissioned() ? "Decommissioned" : 
						(dndsp.isDecommissionInProgress() ? "Decommission In Progress" : 
						(dndsp.isAlive() ? "Live" : "Dead"))));
			jsonobj.put("num_of_active_connections", 
						dndsp.getXceiverCount());
			
			//response, json format
			response.setContentType("application/json");
			out.print(jsonobj);
			out.flush();
			return ;
		}
		else if(key.equalsIgnoreCase("status")){
		// request: 
		//		http://host:port/monitor?class=datanode&storageID=...&key=status
		// response: 
		//		"cpu-status": see below, for details
		//		"mem-status":
		//		"net-status":
		//		"io-status":
		/* output sample
			{
				"io-status":
					[
						{
							"diskname":"sda",
							"cur-rate":8.0E-4,
							"min-rate":0,
							"avg-rate":0.00284393063583815,
							"max-rate":0.1768
						}
					],
				"net-status":
					[
						{
							"cur-tx-rate":470.8,
							"cur-rx-rate":2743.4,
							"cardname":"eth0",
							"avg-tx-rate":77645.60462427746,
							"avg-rx-rate":4188.277456647399,
							"min-tx-rate":13.2,
							"min-rx-rate":98.4,
							"max-tx-rate":1.18730026E7,
							"max-rx-rate":203278.2,
							"duplex":"full",
							"bandwidth":1000
						}
					],
				"mem-status":
					{
						"cur-rate":0.39310484737585294,
						"min-rate":0.3710914981611185,
						"avg-rate":0.382637524894124,
						"max-rate":0.3934051344060644
					},
				"cpu-status":
					{
						"cur-rate":1,
						"min-rate":0.999,
						"avg-rate":0.9999971102066058,
						"max-rate":1
					}
			}
		*/
			ServernodeCPUStatus cpuStatus = dndsp.getCPUStatus();
			ServernodeMEMStatus memStatus = dndsp.getMEMStatus();
			ServernodeNETStatus[] netStatus = dndsp.getNETStatus();
			ServernodeIOStatus[] ioStatus = dndsp.getIOStatus();
			
			jsonobj.put("cpu_status", 
						new JSONObject()
							.put("cur_rate", cpuStatus.getCurCPURate())
							.put("avg_rate", cpuStatus.getAvgCPURate())
							.put("max_rate", cpuStatus.getMaxCPURate())
							.put("min_rate", cpuStatus.getMinCPURate()));
			jsonobj.put("mem_status", 
						new JSONObject()
							.put("cur_rate", memStatus.getCurMEMRate())
							.put("avg_rate", memStatus.getAvgMEMRate())
							.put("max_rate", memStatus.getMaxMEMRate())
							.put("min_rate", memStatus.getMinMEMRate()));
			JSONArray jsonNetArray = new JSONArray();
			for(int i = 0; i < netStatus.length; i++){
				jsonNetArray.put(
					new JSONObject()
						.put("cardname", netStatus[i].getCardname())
						.put("bandwidth", netStatus[i].getBandwidth())
						.put("duplex", netStatus[i].getDuplex())
						.put("cur_rx_rate", netStatus[i].getCurNETRxRate())
						.put("cur_tx_rate", netStatus[i].getCurNETTxRate())
						.put("avg_rx_rate", netStatus[i].getAvgNETRxRate())
						.put("avg_tx_rate", netStatus[i].getAvgNETTxRate())
						.put("max_rx_rate", netStatus[i].getMaxNETRxRate())
						.put("max_tx_rate", netStatus[i].getMaxNETTxRate())
						.put("min_rx_rate", netStatus[i].getMinNETRxRate())
						.put("min_tx_rate", netStatus[i].getMinNETTxRate()));
			}
			jsonobj.put("net_status", 
						jsonNetArray);
			
			JSONArray jsonIOArray = new JSONArray();
			for(int j = 0; j < ioStatus.length; j++){
				jsonIOArray.put(
					new JSONObject()
						.put("diskname", ioStatus[j].getDiskname())
						.put("cur_rate", ioStatus[j].getCurIORate())
						.put("avg_rate", ioStatus[j].getAvgIORate())
						.put("max_rate", ioStatus[j].getMaxIORate())
						.put("min_rate", ioStatus[j].getMinIORate()));
			}
			jsonobj.put("io_status", 
						jsonIOArray);
			
			//response, json format
			response.setContentType("application/json");
			out.print(jsonobj);
			out.flush();
			return ;
		}
		/*TODO
		else if(key.equalsIgnoreCase("blocks")){
		// http://host:port/monitor?class=datanode&storageID=...&key=blocks
		}
		*/
		else{
			doBadRequest(request, response, "key invalid(class: datanode)");
			return ;
		}
	}
	
	
	/** * * * * * * * * * * * * * * * * * * * * * * * * * *
	request for file:
		http://host:port/monitor?class=file&path=...&key=... (see below, for details)
		
	NOTE: 
		1. file includes directory and non-directory.
		2. a file is identified by its path(absolute path).
	* * * * * * * * * * * * * * * * * * * * * * * * * * **/
	private void getFileInfo(
			HttpServletRequest request, HttpServletResponse response)
			throws Exception{
		NameNode nn = (NameNode)getServletContext().getAttribute("name.node");
		FSNamesystem fsn = nn.getNamesystem();
		PrintWriter out = response.getWriter();
		JSONObject jsonobj = new JSONObject();

		//check path
		String path = JspHelper.validatePath(StringEscapeUtils.unescapeHtml(
				request.getParameter("path")));
		if(path == null){
			doBadRequest(request, response, "need path(class: file)");
			return ;
		}
//		path = repairPath(path);
		
		//get file status
		HdfsFileStatus targetStatus = fsn.getFileInfo(path, true);
		if(targetStatus == null){
			doBadRequest(request, response, "path(" + path + ") invalid(class: file)");
			return ;
		}
		
		String key = StringEscapeUtils.unescapeHtml(
				request.getParameter("key"));
		if(key == null || key.length() == 0){
			doBadRequest(request, response, "need key(class: file)");
			return ;
		}
		else if(key.equalsIgnoreCase("name")){
		// request: 
		//		http://host:port/monitor?class=file&path=...&key=name (directory, non-directory)
		// response: 
		//		"full-name":STRING
		//		"local-name":STRING
		//		"file-type":STRING
		/* output sample
		 	{
		 		"local-name":"8MB",
		 		"full-name":"/user/hadoop/input/8MB",
		 		"file-type":"file"
		 	}
		 */
			jsonobj.put("full_name", 
						path);
			String localName;
			if(path.equalsIgnoreCase("/"))
				localName = "/";
			else{
				String[] ss = path.split("/");
				localName = ss[ss.length - 1];
			}
			jsonobj.put("local_name", 
						localName);
			jsonobj.put("file_type", 
						(targetStatus.isDir() ? "directory" : "file"));
			
			//response, json format
			response.setContentType("application/json");
			out.print(jsonobj);
			out.flush();
			return ;
		}
		else if(key.equalsIgnoreCase("time")){
		// request:
		//		http://host:port/monitor?class=file&path=...&key=time (directory, non-directory)
		// response: 
		//		"access-time":LONG
		//		"modification-time":LONG
		/* output sample
		  	{
				"access-time":1397133089784,
				"modification-time":1397133095478
			}
		 */
			jsonobj.put("modification_time", 
						targetStatus.getModificationTime());
			jsonobj.put("access_time", 
						targetStatus.getAccessTime());
			
			//response, json format
			response.setContentType("application/json");
			out.print(jsonobj);
			out.flush();
			return ;
		}
		else if(key.equalsIgnoreCase("permission")){
		// request:
		//		http://host:port/monitor?class=file&path=...&key=permission (directory, non-directory)
		// response: 
		//		"permission":STRING
		//		"owner":STRING
		//		"group":STRING
		/* output sample
		 	{
				"owner":"hadoop",
				"group":"supergroup",
				"permission":"rw-r--r--"
			}
		 */
			jsonobj.put("permission", 
						targetStatus.getPermission().toString());
			jsonobj.put("owner", 
						targetStatus.getOwner());
			jsonobj.put("group", 
						targetStatus.getGroup());
			
			//response, json format
			response.setContentType("application/json");
			out.print(jsonobj);
			out.flush();
			return ;
		}
		else if(key.equalsIgnoreCase("storage")){
		// request: 
		//		http://host:port/monitor?class=file&path=...&key=storage (directory, non-directory)
		// response: 
		//		see below, for details			
			if(targetStatus.isDir()){
				//for directory
				// request:
				//		http://host:port/monitor?class=file&path=...&key=storage
				// response: 
				//		"namespace-quota":LONG
				//		"namespace-consumed":
				//			{
				//				"file-count":LONG
				//				"directory-count":LONG
				//			}
				//		"diskspace-quota":LONG
				//		"diskspace-consumed":LONG
				/* output sample
					{
	 					"namespace-quota":-1, // -1 means no limit
	 					"namespace-consumed":{"directory-count":1,"file-count":2},
	 					"diskspace-consumed":69913600, // in byte
	 					"diskspace-quota":-1
 					}
				 */
				ContentSummary contentSummary = fsn.dir.getContentSummary(path);
				jsonobj.put("namespace_quota", 
							contentSummary.getQuota());
				jsonobj.put("namespace_consumed", 
							new JSONObject()
								.put("file_count", contentSummary.getFileCount())
								.put("directory_count", contentSummary.getDirectoryCount()));
				jsonobj.put("diskspace_quota", 
							contentSummary.getSpaceQuota());
				jsonobj.put("diskspace_consumed", 
							contentSummary.getSpaceConsumed());
			}
			else{
				//for non-directory
				// request: 
				//		http://cumulus:50070/monitor?class=file&path=...&key=storage
				// response:
				//		"file-size":LONG // in byte
				//		"replication":SHORT
				//		"diskspace-consumed":LONG // in byte
				//		"block-size":LONG // 0 means not set/used
				//		"block-count":INT
				/* output sample
					{
						"block-count":5,
						"block-size":0, // 0 means not set/used
						"replication":1,
						"diskspace-consumed":13982720,
						"file-size":8388608
					}
				*/
				INodeFile filenode = fsn.dir.getFileINode(path);
				jsonobj.put("file_size", 
							filenode.getFileSize());
				jsonobj.put("replication", 
							targetStatus.getReplication());
				jsonobj.put("diskspace_consumed", 
							filenode.diskspaceConsumed());
				jsonobj.put("block_size", 
							targetStatus.getBlockSize());
				jsonobj.put("block_count", 
							filenode.numBlocks());
			}
			
			//response, json format
			response.setContentType("application/json");
			out.print(jsonobj);
			out.flush();
			return ;
		}
		else if(key.equalsIgnoreCase("children")){
		// request:
		//		http://host:port/monitor?class=file&path=...&key=children (directory)
		// response: 
		//		"num-of-child":INT
		//		"child-local-name":STRING[] //local name of children
		/* output sample
			 {
			 	"num-of-child":2,
			 	"child-local-name":["32MB","8MB"]
			 }
		 */
			if(!targetStatus.isDir()){
				doBadRequest(request, response, 
						path + " is file, directory needed(class: file, key: children)");
				return ;
			}
			
			DirectoryListing thisListing = fsn.dir.getListing(
					path, HdfsFileStatus.EMPTY_NAME, false);
			HdfsFileStatus[] children = null;
			if(thisListing != null)
				children = thisListing.getPartialListing();
			
			if(thisListing == null || children == null){
				jsonobj.put("num_of_child", 
							0);
				jsonobj.put("child_local_name", 
							JSONObject.NULL);
			}
			else{
				jsonobj.put("num_of_child", 
							children.length);
				JSONArray childArray = new JSONArray();
				for(int i = 0; i < children.length; i++)
					childArray.put(children[i].getLocalName());
				jsonobj.put("child_local_name", 
							childArray);
			}
			
			//response, json format
			response.setContentType("application/json");
			out.print(jsonobj);
			out.flush();
			return ;
		}
		else if(key.equalsIgnoreCase("matrix")){
		// request: 
		//		http://host:port/monitor?class=file&path=...&key=matrix (non-directory)
		// response: 
		//		"matrix-row":BYTE
		//		"matrix-col"BYTE
		//		"coding-matrix":BYTE[][]
		/* output sample
		 	{
				"coding-matrix":
					[
						[-89,71,-70,122,1],
						[122,-70,71,-89,-114],
						[-70,122,-89,71,-12]
					],
				"matrix-row":3,
				"matrix-col":5
			}
		 */
			if(targetStatus.isDir()){
				doBadRequest(request, response, 
						path + " is directory, file needed(class: file, key: children)");
				return ;
			}
			
			INodeFile filenode = fsn.dir.getFileINode(path);
			CodingMatrix matrix = filenode.getMatrix();
			jsonobj.put("matrix_row", 
						matrix.getRow());
			jsonobj.put("matrix_col", 
						matrix.getColumn());
			jsonobj.put("coding_matrix", 
						matrix.getCodingmatrix());
			
			//response, json format
			response.setContentType("application/json");
			out.print(jsonobj);
			out.flush();
			return ;
		}
		else if(key.equalsIgnoreCase("blocks")){
		// request: 
		//		http://host:port/monitor?class=directory&path=...&key=blocks (non-directory)
		// response:
		//		"num-of-blocks":INT
		//		"block-id":LONG[]
		/* output sample
		 	{
			 	"num-of-blocks":5,
			 	"block-id":
			 		[
				 		6575385822766126681,
				 		-2828137763887586036,
				 		-4994485102803731246,
				 		-4263054064009953871,
				 		-3156775476468273954
			 		]
		 	}
		 */
			if(targetStatus.isDir()){
				doBadRequest(request, response, 
						path + " is directory, file needed(class: file, key: blocks)");
				return ;
			}
			
			INodeFile filenode = fsn.dir.getFileINode(path);
			BlockInfo[] blocks = filenode.getBlocks();
			jsonobj.put("num_of_blocks", 
						blocks.length);
			JSONArray blockArray = new JSONArray();
			for(int i = 0; i < blocks.length; i++)
				blockArray.put(blocks[i].getBlockId());
			jsonobj.put("block_id", 
						blockArray);
			
			//response, json format
			response.setContentType("application/json");
			out.print(jsonobj);
			out.flush();
			return ;
		}
		else{
			doBadRequest(request, response, "key invalid(class: file)");
			return ;
		}
	}
	
	
	/** * * * * * * * * * * * * * * * * * * * * * * * * * *
	request for block:
		http://host:port/monitor?class=block&path=...&blockID=...&key=blockinfo
		http://host:port/monitor?class=block&path=...&blockID=...&key=replicas
	* * * * * * * * * * * * * * * * * * * * * * * * * * **/
	private void getBlockInfo(
			HttpServletRequest request, HttpServletResponse response)
			throws Exception{
		NameNode nn = (NameNode)getServletContext().getAttribute("name.node");
		FSNamesystem fsn = nn.getNamesystem();
		PrintWriter out = response.getWriter();
		JSONObject jsonobj = new JSONObject();
		
		//check path
		String path = JspHelper.validatePath(StringEscapeUtils.unescapeHtml(
				request.getParameter("path")));
		if(path == null){
			doBadRequest(request, response, "need path(class: block)");
			return ;
		}
		
//		path = repairPath(path);
		if(!fsn.dir.exists(path)){
			doBadRequest(request, response, "path(" + path + ") invalid(class: block)");
			return ;
		}
		if(fsn.dir.isDir(path)){
			doBadRequest(request, response, 
					path + " is directory, file needed(class: block)");
			return ;
		}
		
		//check blockID
		String blockIDStr = request.getParameter("blockID");
		if(blockIDStr == null || blockIDStr.length() == 0){
			doBadRequest(request, response, "need blockID(class: block)");
			return ;
		}
		
		Long blockID = null;
		try{
			blockID = JspHelper.validateLong(blockIDStr);
		}catch(NumberFormatException e){
			doBadRequest(request, response, "blockID invalid. " + 
				"blockID is a long number and should be all digit(class: block)");
			return ;
		}
		
		INodeFile filenode = fsn.dir.getFileINode(path);
		BlockInfo blockinfo = filenode.getBlockByBlockID(blockID);
		if(blockinfo == null){
			doBadRequest(request, response, "blockID invalid(class: block)");
			return ;
		}
		
		String key = StringEscapeUtils.unescapeHtml(
				request.getParameter("key"));
		if(key == null || key.length() == 0){
			doBadRequest(request, response, "need key(class: block)");
			return ;
		}
		else if(key.equalsIgnoreCase("blockinfo")){
			// request: 
			//		http://host:port/monitor?class=block&path=...&blockID=...&key=blockinfo
			// response: 
			//		"block-id":LONG,
			//		"block-name":STRING,
			//		"generation-stamp":LONG,
			//		"num-of-bytes":LONG
			/* output sample
			 	{
					"block-id":-5083924400203472817,
					"block-name":"blk_-5083924400203472817",
					"generation-stamp":1002,
					"num-of-bytes":11186176
				}
			*/
			jsonobj.put("block_id", 
						blockinfo.getBlockId());
			jsonobj.put("block_name", 
						blockinfo.getBlockName());
			jsonobj.put("generation_stamp", 
						blockinfo.getGenerationStamp());
			jsonobj.put("num_of_bytes", 
						blockinfo.getNumBytes());
			
			//response, json format
			response.setContentType("application/json");
			out.print(jsonobj);
			out.flush();
			return ;
		}
		else if(key.equalsIgnoreCase("replicas")){
			// request:
			//		http://host:port/monitor?class=block&path=...&blockID=...&key=replica
			// response: 
			//		"num-of-replicas":INT,
			//		"replicas":STRING[]
			/* output sample
				{
					"num-of-replicas":1,
					"replicas":
						[
							"DS-895206706-114.212.82.16-50010-1397127383674"
						]
				}
			 */
			jsonobj.put("num_of_replicas", 
						blockinfo.numNodes());
			JSONArray nodeArray = new JSONArray();
			for(int i = 0; i < blockinfo.numNodes(); i++)
				nodeArray.put(blockinfo.getDatanode(i).getStorageID());
			jsonobj.put("replicas", 
						nodeArray);
			
			//response, json format
			response.setContentType("application/json");
			out.print(jsonobj);
			out.flush();
			return ;
		}
		else{
			doBadRequest(request, response, "key invalid(class: block)");
			return ;
		}
	}
	
	
	/** * * * * * * * * * * * * * * * * * * * * * * * * * *
	request for log:
		http://host:port/monitor?class=log&key=namenode
		http://host:port/monitor?class=log&key=datanode&storageID=...
	* * * * * * * * * * * * * * * * * * * * * * * * * * **/
	private void getLogInfo(
			HttpServletRequest request, HttpServletResponse response)
			throws Exception{
		NameNode nn = (NameNode)getServletContext().getAttribute("name.node");
		PrintWriter out = response.getWriter();
		JSONObject jsonobj = new JSONObject();
		
		String key = StringEscapeUtils.unescapeHtml(
				request.getParameter("key"));
		if(key == null || key.length() == 0){
			doBadRequest(request, response, "need key(class: log)");
			return ;
		}
		else if(key.equalsIgnoreCase("namenode")){
		// request: 
		//		http://host:port/monitor?class=log&key=namenode
		// response: 
		//		num-of-logfiles:INT
		//		logfile-link:STRING[]
		/* output sample
				see below, for details
		*/
			String logDir = System.getProperty("hadoop.log.dir");
			File[] fileList = new File(logDir).listFiles();
			
			if(fileList == null || fileList.length == 0){
				/* output sample
			 	{
			 		"logfile-link":null,
			 		"num-of-logfiles":0
			 	}
			    */
				jsonobj.put("num_of_logfiles", 
							0);
				jsonobj.put("logfile_link", 
							JSONObject.NULL);
			}
			else{
				/* output sample
				{
					"num-of-logfiles":5,
					"logfile-link":
						[
							"http://114.212.81.5:50070/logs/hadoop-hadoop-namenode-cumulus.log",
							"http://114.212.81.5:50070/logs/hadoop-hadoop-secondarynamenode-cumulus.log",
							"http://114.212.81.5:50070/logs/hadoop-hadoop-namenode-cumulus.out",
							"http://114.212.81.5:50070/logs/hadoop-hadoop-secondarynamenode-cumulus.out",
							"http://114.212.81.5:50070/logs/SecurityAuth.audit"
						]
				}
			   */
				String[] ss = nn.getNameNodeAddress().getAddress().toString().split("/");
				String linkPrefix = "http://" + ss[ss.length-1] + ":" + 
								nn.getHttpAddress().getPort() + "/logs/";
				
				jsonobj.put("num_of_logfiles", 
							fileList.length);
				JSONArray logArray = new JSONArray();
				for(File f : fileList){
					logArray.put(linkPrefix + f.getName());
				}
				jsonobj.put("logfile_link", 
							logArray);
			}
			
			//response, json format
			response.setContentType("application/json");
			out.print(jsonobj);
			out.flush();
			return ;
		}
		else if(key.equalsIgnoreCase("datanode")){
			// request: 
			//		http://host:port/monitor?class=log&key=datanode&storageID=...
			// response: 
			//		redirect to: http://datanode-host:port/monitor?key=log
			// see: 
			//		org.apache.hadoop.hdfs.server.datanode.DatanodeMonitorServlet.java
			/* output sample (a redirect response)
					HTTP/1.1 302 Found
					Location: http://114.212.82.13:50075/monitor?key=log
			 */
			String storageID = StringEscapeUtils.unescapeHtml(
					request.getParameter("storageID"));
			if(storageID == null || storageID.length() == 0){
				doBadRequest(request, response, "need storageID(class: log)");
				return ;
			}
			
			DatanodeDescriptor dndsp = nn.getNamesystem().getDatanode(storageID);
			if(dndsp == null){
				doBadRequest(request, response, "storageID invalid(class: log)");
				return ;
			}
			
			String redirectLocation = "http://" + dndsp.getHost() + 
						":" + dndsp.getInfoPort() + "/monitor?key=log";
			response.sendRedirect(redirectLocation);
			return ;
		}
		else{
			doBadRequest(request, response, "key invalid(class: log)");
			return ;
		}
	}
	

	/** * * * * * * * * * * * * * * * * * * * * * * * * * *
	request for conf:
		http://host:port/monitor?class=conf&key=namenode
		http://host:port/monitor?class=conf&key=datanode&storageID=...
	* * * * * * * * * * * * * * * * * * * * * * * * * * **/
	private void getConfigurationInfo(
			HttpServletRequest request, HttpServletResponse response)
			throws Exception{
		NameNode nn = (NameNode)getServletContext().getAttribute("name.node");
		PrintWriter out = response.getWriter();
		JSONObject jsonobj = new JSONObject();
		
		String key = StringEscapeUtils.unescapeHtml(
				request.getParameter("key"));
		if(key == null || key.length() == 0){
			doBadRequest(request, response, "need key(class: conf)");
			return ;
		}
		else if(key.equalsIgnoreCase("namenode")){
			// request: 
			//		http://host:port/monitor?class=conf&key=namenode
			// response: 
			//		num-of-conffiles:INT
			//		conffile-link:STRING[]
			/* output sample
				{
					"num-of-conffiles":1,
					"conffile-link":["http://114.212.81.5:50070/conf"]
				}
			 */
			
			String[] ss = nn.getNameNodeAddress().getAddress().toString().split("/");
			String confLocation = "http://" + ss[ss.length-1] + ":" + 
							nn.getHttpAddress().getPort() + "/conf";
			
			jsonobj.put("num_of_conffiles", 
						1);
			jsonobj.put("conffile_link", 
						new JSONArray().put(confLocation));
			
			//response, json format
			response.setContentType("application/json");
			out.print(jsonobj);
			out.flush();
			return ;
		}
		else if(key.equalsIgnoreCase("datanode")){
			// request: 
			//		http://host:port/monitor?class=conf&key=datanode&storageID=...
			// response: 
			//		num-of-conffiles:INT
			//		conffile-link:STRING[]
			/* output sample
				{
					"num-of-conffiles":1,
					"conffile-link":["http://114.212.82.2:50075/conf"]
				}
			 */
			String storageID = StringEscapeUtils.unescapeHtml(
					request.getParameter("storageID"));
			if(storageID == null || storageID.length() == 0){
				doBadRequest(request, response, "need storageID(class: conf)");
				return ;
			}
			
			DatanodeDescriptor dndsp = nn.getNamesystem().getDatanode(storageID);
			if(dndsp == null){
				doBadRequest(request, response, "storageID invalid(class: conf)");
				return ;
			}
			
			String confLocation = "http://" + dndsp.getHost() + ":" + 
							dndsp.getInfoPort() + "/conf";
			
			jsonobj.put("num_of_conffiles", 
						1);
			jsonobj.put("conffile_link", 
						new JSONArray().put(confLocation));
			
			//response, json format
			response.setContentType("application/json");
			out.print(jsonobj);
			out.flush();
			return ;
		}
		else{
			doBadRequest(request, response, "key invalid(class: conf)");
			return ;
		}
	}
}
