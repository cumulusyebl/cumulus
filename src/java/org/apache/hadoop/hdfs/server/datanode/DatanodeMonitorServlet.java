
package org.apache.hadoop.hdfs.server.datanode;
/**
 * author: xianyu
 * date: 2014-04-10
 * */

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang.StringEscapeUtils;
import org.json.*;

public class DatanodeMonitorServlet extends HttpServlet{
	/** For java.io.Serializable */
	private static final long serialVersionUID = 1L;
	
	private void doBadRequest(
			HttpServletRequest request, HttpServletResponse response, String msg)
			throws IOException{
		JSONObject jsonobj = new JSONObject();
		jsonobj.put("errmsg", msg);
		
		response.setContentType("application/json");
		PrintWriter out = response.getWriter();
		out.print(jsonobj);
		out.flush();
	}
	
	/**
	* Service a GET request as described below.
	* Request:
	* 	GET http://<host>:<port>/monitor?key=log HTTP/1.1
	* */
	public void doGet(
			HttpServletRequest request, HttpServletResponse response)
			throws IOException {
		JSONObject jsonobj = new JSONObject();
		PrintWriter out = response.getWriter();
		
		String key = StringEscapeUtils.unescapeHtml(
				request.getParameter("key"));
		if(key == null || key.length() == 0){
			doBadRequest(request, response, "need key");
			return ;
		}
		else if(key.equalsIgnoreCase("log")){
			// request:
			//		http://<host>:<port>/monitor?key=log
			// response: 
			//		"num-of-logfiles" : INT,
			//		"logfile-link" : STRING[]
			/* output sample
					see below, for details
			 */
			String logDir = System.getProperty("hadoop.log.dir");
			File[] logList = new File(logDir).listFiles();
			
			if(logList == null || logList.length == 0){
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
					"num-of-logfiles":3,
					"logfile-link":
					[
						"http://114.212.82.13:50075/logs/hadoop-hadoop-datanode-cloudera.out",
						"http://114.212.82.13:50075/logs/SecurityAuth.audit",
						"http://114.212.82.13:50075/logs/hadoop-hadoop-datanode-cloudera.log"
					]
				}
			    */
				String linkPrefix = "http://" + 
						request.getLocalAddr() + ":" + request.getLocalPort() + "/logs/";
				
				jsonobj.put("num_of_logfiles", 
							logList.length);
				JSONArray logArray = new JSONArray();
				for(File f : logList){
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
		else{
			doBadRequest(request, response, "key invalid");
			return ;
		}
	}
}