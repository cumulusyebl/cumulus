/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.protocol;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.NodeBase;
import org.apache.hadoop.util.StringUtils;

import org.apache.avro.reflect.Nullable;

import org.apache.hadoop.hdfs.server.monitor.*;//add by xianyu

/** 
 * DatanodeInfo represents the status of a DataNode.
 * This object is used for communication in the
 * Datanode Protocol and the Client Protocol.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class DatanodeInfo extends DatanodeID implements Node {
  protected long capacity;
  protected long dfsUsed;
  protected long remaining;
  
  /************ add by xianyu ************/
  protected ServernodeCPUStatus cpuStatus;
  protected ServernodeMEMStatus memStatus;
  protected ServernodeNETStatus[] netStatus;
  protected ServernodeIOStatus[] ioStatus;
  /***************************************/
  
  //removed by xianyu
  /*
  protected long cpuUsed;  //ww add
  protected long memUsed;  //ww add  
  protected long ioUsed;   //ww add
  */
  protected long lastUpdate;
  protected int xceiverCount;
  protected String location = NetworkTopology.DEFAULT_RACK;

  /** HostName as supplied by the datanode during registration as its 
   * name. Namenode uses datanode IP address as the name.
   */
  @Nullable
  protected String hostName = null;
  
  // administrative states of a datanode
  public enum AdminStates {NORMAL, DECOMMISSION_INPROGRESS, DECOMMISSIONED; }
  @Nullable
  protected AdminStates adminState;


  public DatanodeInfo() {
    super();
    adminState = null;
  }
  
  public DatanodeInfo(DatanodeInfo from) {
    super(from);
    this.capacity = from.getCapacity();
    this.dfsUsed = from.getDfsUsed();
    this.remaining = from.getRemaining();
    
    /******** add by xianyu ********/
    this.cpuStatus = from.cpuStatus;
    this.memStatus = from.memStatus;
    this.netStatus = from.netStatus;
    this.ioStatus = from.ioStatus;
    /*******************************/
    
    //removed by xianyu
    /*
    this.cpuUsed=from.getCpuUsed();   //ww added
    this.memUsed=from.getMemUsed();   //ww added
    this.ioUsed=from.getIoUsed();     //ww added
    */
    this.lastUpdate = from.getLastUpdate();
    this.xceiverCount = from.getXceiverCount();
    this.location = from.getNetworkLocation();
    this.adminState = from.adminState;
    this.hostName = from.hostName;
  }

  public DatanodeInfo(DatanodeID nodeID) {
    super(nodeID);
    this.capacity = 0L;
    this.dfsUsed = 0L;
    this.remaining = 0L;
    
    /******** add by xianyu ********/
    this.cpuStatus = null;
    this.memStatus = null;
    this.netStatus = null;
    this.ioStatus = null;
    /*******************************/
    
    //removed by xianyu
    /*
    this.cpuUsed = 0L;   //ww added;
    this.memUsed = 0L;   //ww added;
    this.ioUsed = 0L;    //ww added;
    */
    this.lastUpdate = 0L;
    this.xceiverCount = 0;
    this.adminState = null;    
  }
  
  protected DatanodeInfo(DatanodeID nodeID, String location, String hostName) {
    this(nodeID);
    this.location = location;
    this.hostName = hostName;
  }
  
  /** The raw capacity. */
  public long getCapacity() { return capacity; }
  
  /** The used space by the data node. */
  public long getDfsUsed() { return dfsUsed; }

  /** The used space by the data node. */
  public long getNonDfsUsed() { 
    long nonDFSUsed = capacity - dfsUsed - remaining;
    return nonDFSUsed < 0 ? 0 : nonDFSUsed;
  }

  /** The used space by the data node as percentage of present capacity */
  public float getDfsUsedPercent() { 
    if (capacity <= 0) {
      return 100;
    }

    return ((float)dfsUsed * 100.0f)/(float)capacity; 
  }

  /** The raw free space. */
  public long getRemaining() { return remaining; }

  /** The remaining space as percentage of configured capacity. */
  public float getRemainingPercent() { 
    if (capacity <= 0) {
      return 0;
    }

    return ((float)remaining * 100.0f)/(float)capacity; 
  }

  
  /************* add by xianyu *************/
  /** get the cpu status of datanode */
  public ServernodeCPUStatus getCPUStatus(){
	  return cpuStatus;
  }
  /** get the mem status of datanode */
  public ServernodeMEMStatus getMEMStatus(){
	  return memStatus;
  }
  /** get the net status of datanode */
  public ServernodeNETStatus[] getNETStatus(){
	  return netStatus;
  }
  /** get the io status of datanode */
  public ServernodeIOStatus[] getIOStatus(){
	  return ioStatus;
  }
  /*****************************************/
  
  //removed by xianyu
//  /** The used cpu percentage by the data node. */
//  public long getCpuUsed() { return cpuUsed; }    // ww added
//  
//     /** The used mem percentage by the data node. */
//  public long getMemUsed() { return memUsed; }    // ww added
//
//    /** The used io percentage by the data node. */
//  public long getIoUsed() { return ioUsed; }      // ww added
  
  
  /** The time when this information was accurate. */
  public long getLastUpdate() { return lastUpdate; }
  
  /** number of active connections */
  public int getXceiverCount() { return xceiverCount; }

  /** Sets raw capacity. */
  public void setCapacity(long capacity) { 
    this.capacity = capacity; 
  }

  /** Sets raw free space. */
  public void setRemaining(long remaining) { 
    this.remaining = remaining; 
  }
  
 
  /****************** add by xianyu *******************/
  /** set the cpu status of datanode */
  public void setCPUStatus(ServernodeCPUStatus status){
	  this.cpuStatus = status;
  }
  /** set the mem status of datanode */
  public void setMEMStatus(ServernodeMEMStatus status){
	  this.memStatus = status;
  }
  /** set the net status of datanode */
  public void setNETStatus(ServernodeNETStatus[] status){
	  this.netStatus = status;
  }
  /** set the io status of datanode */
  public void setIOStatus(ServernodeIOStatus[] status){
	  this.ioStatus = status;
  }
  /****************************************************/
  
  //removed by xianyu
//  public void setCpuUsed(long cpuUsed) {    //ww added
//	  this.cpuUsed = cpuUsed;
//    }
//     /** Sets mem used percentage. */
//  public void setMemUsed(long memUsed) {      //ww added
//	  this.memUsed = memUsed; 
//	  }
//    /** Sets io used percentage. */
//  public void setIoUsed(long ioUsed) {      //ww added
//	  this.ioUsed = ioUsed; 
//	  }

  /** Sets time when this information was accurate. */
  public void setLastUpdate(long lastUpdate) { 
    this.lastUpdate = lastUpdate; 
  }

  /** Sets number of active connections */
  public void setXceiverCount(int xceiverCount) { 
    this.xceiverCount = xceiverCount; 
  }

  /** rack name */
  public synchronized String getNetworkLocation() {return location;}
    
  /** Sets the rack name */
  public synchronized void setNetworkLocation(String location) {
    this.location = NodeBase.normalize(location);
  }
  
  public String getHostName() {
    return (hostName == null || hostName.length()==0) ? getHost() : hostName;
  }
  
  public void setHostName(String host) {
    hostName = host;
  }
  
  /** A formatted string for reporting the status of the DataNode. */
  public String getDatanodeReport() {
    StringBuilder buffer = new StringBuilder();
    long c = getCapacity();
    long r = getRemaining();
    long u = getDfsUsed();
    //long cpuUsed = getCpuUsed();       //ww added;
    //long memUsed = getMemUsed();       //ww added;
    //long ioUsed = getIoUsed();         //ww added;
    long nonDFSUsed = getNonDfsUsed();
    float usedPercent = getDfsUsedPercent();
    float remainingPercent = getRemainingPercent();
    String hostName = NetUtils.getHostNameOfIP(name);

    buffer.append("Name: "+ name);
    if(hostName != null)
      buffer.append(" (" + hostName + ")");
    buffer.append("\n");

    if (!NetworkTopology.DEFAULT_RACK.equals(location)) {
      buffer.append("Rack: "+location+"\n");
    }
    buffer.append("Decommission Status : ");
    if (isDecommissioned()) {
      buffer.append("Decommissioned\n");
    } else if (isDecommissionInProgress()) {
      buffer.append("Decommission in progress\n");
    } else {
      buffer.append("Normal\n");
    }
    buffer.append("Configured Capacity: "+c+" ("+StringUtils.byteDesc(c)+")"+"\n");
    buffer.append("DFS Used: "+u+" ("+StringUtils.byteDesc(u)+")"+"\n");
    buffer.append("Non DFS Used: "+nonDFSUsed+" ("+StringUtils.byteDesc(nonDFSUsed)+")"+"\n");
    buffer.append("DFS Remaining: " +r+ " ("+StringUtils.byteDesc(r)+")"+"\n");
    buffer.append("DFS Used%: "+StringUtils.limitDecimalTo2(usedPercent)+"%\n");
    
    /************ add by xianyu ************/
    ServernodeCPUStatus cs = getCPUStatus();
    ServernodeMEMStatus ms = getMEMStatus();
    ServernodeNETStatus ns[] = getNETStatus();
    ServernodeIOStatus ios[] = getIOStatus();
    if(cs != null)
    	buffer.append(getCPUStatus().toString());
    if(ms != null)
    	buffer.append(getMEMStatus().toString());
    if(ns != null && ns.length > 0){
    	for(int i = 0; i < ns.length; i++)
    		buffer.append(ns[i].toString());
    }
    if(ios != null && ios.length > 0){
    	for(int j = 0; j < ios.length; j++)
    		buffer.append(ios[j].toString());
    }
    /***************************************/
    
    //buffer.append("Cpu Used: "+cpuUsed+" ("+StringUtils.byteDesc(cpuUsed)+")"+"\n"); // ww added
    //buffer.append("Mem Used: "+memUsed+" ("+StringUtils.byteDesc(memUsed)+")"+"\n"); // ww added
    //buffer.append("IO Used: "+ioUsed+" ("+StringUtils.byteDesc(ioUsed)+")"+"\n");    // ww added
    buffer.append("DFS Remaining%: "+StringUtils.limitDecimalTo2(remainingPercent)+"%\n");
    buffer.append("Last contact: "+new Date(lastUpdate)+"\n");
    return buffer.toString();
  }

  /** A formatted string for printing the status of the DataNode. */
  public String dumpDatanode() {
    StringBuilder buffer = new StringBuilder();
    long c = getCapacity();
    long r = getRemaining();
    long u = getDfsUsed();
    //long cpuUsed = getCpuUsed();       //ww added;
    //long memUsed = getMemUsed();       //ww added;
    //long ioUsed = getIoUsed();         //ww added;
    buffer.append(name);
    if (!NetworkTopology.DEFAULT_RACK.equals(location)) {
      buffer.append(" "+location);
    }
    if (isDecommissioned()) {
      buffer.append(" DD");
    } else if (isDecommissionInProgress()) {
      buffer.append(" DP");
    } else {
      buffer.append(" IN");
    }
    buffer.append(" " + c + "(" + StringUtils.byteDesc(c)+")");
    buffer.append(" " + u + "(" + StringUtils.byteDesc(u)+")");
    
    /************ add by xianyu ************/
    ServernodeCPUStatus cs = getCPUStatus();
    ServernodeMEMStatus ms = getMEMStatus();
    ServernodeNETStatus[] ns = getNETStatus();
    ServernodeIOStatus[] ios = getIOStatus();
    if(cs != null)
    	buffer.append(" " + cs.toDump());
    if(ms != null)
    	buffer.append(" " + ms.toDump());
    if(ns != null && ns.length > 0){
    	for(int i = 0; i < ns.length; i++)
    		buffer.append(" " + ns[i].toDump());
    }
    if(ios != null && ios.length > 0){
    	for(int j = 0; j < ios.length; j++)
    		buffer.append(" " + ios[j].toDump());
    }
    /***************************************/
    
    //buffer.append(" "+cpuUsed+" ("+StringUtils.byteDesc(cpuUsed)+")"); // ww added
    //buffer.append(" "+memUsed+" ("+StringUtils.byteDesc(memUsed)+")"); // ww added
    //buffer.append(" "+ioUsed+" ("+StringUtils.byteDesc(ioUsed)+")");    // ww added
    buffer.append(" " + StringUtils.limitDecimalTo2(((1.0*u)/c)*100)+"%");
    buffer.append(" " + r + "(" + StringUtils.byteDesc(r)+")");
    buffer.append(" " + new Date(lastUpdate));
    return buffer.toString();
  }

  /**
   * Start decommissioning a node.
   * old state.
   */
  public void startDecommission() {
    adminState = AdminStates.DECOMMISSION_INPROGRESS;
  }

  /**
   * Stop decommissioning a node.
   * old state.
   */
  public void stopDecommission() {
    adminState = null;
  }

  /**
   * Returns true if the node is in the process of being decommissioned
   */
  public boolean isDecommissionInProgress() {
    return adminState == AdminStates.DECOMMISSION_INPROGRESS;
  }

  /**
   * Returns true if the node has been decommissioned.
   */
  public boolean isDecommissioned() {
    return adminState == AdminStates.DECOMMISSIONED;
  }

  /**
   * Sets the admin state to indicate that decommission is complete.
   */
  public void setDecommissioned() {
    adminState = AdminStates.DECOMMISSIONED;
  }

  /**
   * Retrieves the admin state of this node.
   */
  AdminStates getAdminState() {
    if (adminState == null) {
      return AdminStates.NORMAL;
    }
    return adminState;
  }

  /**
   * Sets the admin state of this node.
   */
  protected void setAdminState(AdminStates newState) {
    if (newState == AdminStates.NORMAL) {
      adminState = null;
    }
    else {
      adminState = newState;
    }
  }

  private transient int level; //which level of the tree the node resides
  private transient Node parent; //its parent

  /** Return this node's parent */
  public Node getParent() { return parent; }
  public void setParent(Node parent) {this.parent = parent;}
   
  /** Return this node's level in the tree.
   * E.g. the root of a tree returns 0 and its children return 1
   */
  public int getLevel() { return level; }
  public void setLevel(int level) {this.level = level;}

  /////////////////////////////////////////////////
  // Writable
  /////////////////////////////////////////////////
  static {                                      // register a ctor
    WritableFactories.setFactory
      (DatanodeInfo.class,
       new WritableFactory() {
         public Writable newInstance() { return new DatanodeInfo(); }
       });
  }

  /** {@inheritDoc} */
  public void write(DataOutput out) throws IOException {
    super.write(out);

    //TODO: move it to DatanodeID once DatanodeID is not stored in FSImage
    out.writeShort(ipcPort);

    out.writeLong(capacity);
    out.writeLong(dfsUsed);
    out.writeLong(remaining);
    
    /************ add by xianyu ************/
    //write datanode status
    if(cpuStatus == null)
    	new ServernodeCPUStatus().write(out);
    else
    	cpuStatus.write(out);
    if(memStatus == null)
    	new ServernodeMEMStatus().write(out);
    else
    	memStatus.write(out);
    if(netStatus == null){
    	out.writeInt(1);
    	new ServernodeNETStatus().write(out);
    }
    else{
    	out.writeInt(netStatus.length);
    	for(int i = 0; i < netStatus.length; i++)
    		netStatus[i].write(out);
    }
    if(ioStatus == null){
    	out.writeInt(1);
    	new ServernodeIOStatus().write(out);
    }
    else{
    	out.writeInt(ioStatus.length);
    	for(int j = 0; j < ioStatus.length; j++)
    		ioStatus[j].write(out);
    }
    /***************************************/
    
    //removed by xianyu
    /*
    out.writeLong(cpuUsed);      //ww added
    out.writeLong(memUsed);      //ww added
    out.writeLong(ioUsed);       //ww added
    */
    out.writeLong(lastUpdate);
    out.writeInt(xceiverCount);
    Text.writeString(out, location);
    Text.writeString(out, hostName == null? "": hostName);
    WritableUtils.writeEnum(out, getAdminState());
  }

  /** {@inheritDoc} */
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);

    //TODO: move it to DatanodeID once DatanodeID is not stored in FSImage
    this.ipcPort = in.readShort() & 0x0000ffff;

    this.capacity = in.readLong();
    this.dfsUsed = in.readLong();
    this.remaining = in.readLong();
    
    /*************** add by xianyu ***************/
    //read datanode status
    this.cpuStatus = ServernodeCPUStatus.read(in);
    this.memStatus = ServernodeMEMStatus.read(in);
    
    int cnt = in.readInt();
    this.netStatus = new ServernodeNETStatus[cnt];
    for(int i = 0; i < cnt; i++)
    	this.netStatus[i] = ServernodeNETStatus.read(in);
    
    cnt = in.readInt();
    this.ioStatus = new ServernodeIOStatus[cnt];
    for(int j = 0; j < cnt; j++)
    	this.ioStatus[j] = ServernodeIOStatus.read(in);
    /*******************************************/
    
    //removed by xianyu
    /*
    this.cpuUsed = in.readLong();     //ww added
    this.memUsed = in.readLong();     //ww added 
    this.ioUsed = in.readLong();      //ww added
    */ 
    this.lastUpdate = in.readLong();
    this.xceiverCount = in.readInt();
    this.location = Text.readString(in);
    this.hostName = Text.readString(in);
    setAdminState(WritableUtils.readEnum(in, AdminStates.class));
  }

  /** Read a DatanodeInfo */
  public static DatanodeInfo read(DataInput in) throws IOException {
    final DatanodeInfo d = new DatanodeInfo();
    d.readFields(in);
    return d;
  }

  @Override
  public int hashCode() {
    // Super implementation is sufficient
    return super.hashCode();
  }
  
  @Override
  public boolean equals(Object obj) {
    // Sufficient to use super equality as datanodes are uniquely identified
    // by DatanodeID
    return (this == obj) || super.equals(obj);
  }
}
