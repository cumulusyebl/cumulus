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
package org.apache.hadoop.hdfs.server.datanode;


import static org.apache.hadoop.hdfs.server.common.Util.now;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.UnknownHostException;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.security.NoSuchAlgorithmException;
import java.security.PrivilegedExceptionAction;
import java.security.SecureRandom;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.BlockReader;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HDFSPolicyProvider;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocol.ClientDatanodeProtocol;
import org.apache.hadoop.hdfs.protocol.CodingMatrix;
import org.apache.hadoop.hdfs.protocol.DataTransferProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.RSCoderProtocol;
import org.apache.hadoop.hdfs.protocol.RecoveryInProgressException;
import org.apache.hadoop.hdfs.protocol.RegeneratingCodeMatrix;
import org.apache.hadoop.hdfs.protocol.UnregisteredNodeException;
import org.apache.hadoop.hdfs.protocol.DataTransferProtocol.BlockConstructionStage;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenSecretManager;
import org.apache.hadoop.hdfs.security.token.block.ExportedBlockKeys;
import org.apache.hadoop.hdfs.server.common.HdfsConstants;
import org.apache.hadoop.hdfs.server.common.IncorrectVersionException;
import org.apache.hadoop.hdfs.server.common.JspHelper;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.Util;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.StartupOption;
import org.apache.hadoop.hdfs.server.datanode.FSDataset.VolumeInfo;
import org.apache.hadoop.hdfs.server.datanode.SecureDataNodeStarter.SecureResources;
import org.apache.hadoop.hdfs.server.datanode.metrics.DataNodeMetrics;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.FileChecksumServlets;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.StreamFile;
import org.apache.hadoop.hdfs.server.protocol.BlockCommand;
import org.apache.hadoop.hdfs.server.protocol.BlockRecoveryCommand;
import org.apache.hadoop.hdfs.server.protocol.CumulusRecoveryCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.DisallowedDatanodeException;
import org.apache.hadoop.hdfs.server.protocol.InterDatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.KeyUpdateCommand;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.RCRecoveryCommand;
import org.apache.hadoop.hdfs.server.protocol.ReplicaRecoveryInfo;
import org.apache.hadoop.hdfs.server.protocol.UpgradeCommand;
import org.apache.hadoop.hdfs.server.protocol.BlockRecoveryCommand.RecoveringBlock;
import org.apache.hadoop.http.HttpServer;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.net.DNS;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.DiskChecker;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.ServicePlugin;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.VersionInfo;
import org.apache.hadoop.util.DiskChecker.DiskErrorException;
import org.apache.hadoop.util.DiskChecker.DiskOutOfSpaceException;
import org.mortbay.util.ajax.JSON;

import java.lang.management.ManagementFactory;  

import javax.management.MBeanServer; 
import javax.management.ObjectName;

import org.apache.hadoop.hdfs.server.monitor.*;//add by xianyu

/**********************************************************
 * DataNode is a class (and program) that stores a set of
 * blocks for a DFS deployment.  A single deployment can
 * have one or many DataNodes.  Each DataNode communicates
 * regularly with a single NameNode.  It also communicates
 * with client code and other DataNodes from time to time.
 *
 * DataNodes store a series of named blocks.  The DataNode
 * allows client code to read these blocks, or to write new
 * block data.  The DataNode may also, in response to instructions
 * from its NameNode, delete blocks or copy blocks to/from other
 * DataNodes.
 *
 * The DataNode maintains just one critical table:
 *   block-> stream of bytes (of BLOCK_SIZE or less)
 *
 * This info is stored on a local disk.  The DataNode
 * reports the table's contents to the NameNode upon startup
 * and every so often afterwards.
 *
 * DataNodes spend their lives in an endless loop of asking
 * the NameNode for something to do.  A NameNode cannot connect
 * to a DataNode directly; a NameNode simply returns values from
 * functions invoked by a DataNode.
 *
 * DataNodes maintain an open server socket so that client code 
 * or other DataNodes can read/write data.  The host/port for
 * this server is reported to the NameNode, which then sends that
 * information to clients or other DataNodes that might be interested.
 *
 **********************************************************/
@InterfaceAudience.Private
public class DataNode extends Configured 
    implements InterDatanodeProtocol, ClientDatanodeProtocol, FSConstants,
    Runnable, DataNodeMXBean {
  public static final Log LOG = LogFactory.getLog(DataNode.class);
  
  static{
    HdfsConfiguration.init();
  }

  public static final String DN_CLIENTTRACE_FORMAT =
        "src: %s" +      // src IP
        ", dest: %s" +   // dst IP
        ", bytes: %s" +  // byte count
        ", op: %s" +     // operation
        ", cliID: %s" +  // DFSClient id
        ", offset: %s" + // offset
        ", srvID: %s" +  // DatanodeRegistration
        ", blockid: %s" + // block id
        ", duration: %s";  // duration time
        
  static final Log ClientTraceLog =
    LogFactory.getLog(DataNode.class.getName() + ".clienttrace");

  /**
   * Use {@link NetUtils#createSocketAddr(String)} instead.
   */
  @Deprecated
  public static InetSocketAddress createSocketAddr(String target
                                                   ) throws IOException {
    return NetUtils.createSocketAddr(target);
  }
  
  public DatanodeProtocol namenode = null;
  public FSDatasetInterface data = null;
  public DatanodeRegistration dnRegistration = null;

  volatile boolean shouldRun = true;
  private LinkedList<Block> receivedBlockList = new LinkedList<Block>();
  private LinkedList<String> delHints = new LinkedList<String>();
  public final static String EMPTY_DEL_HINT = "";
  AtomicInteger xmitsInProgress = new AtomicInteger();
  Daemon dataXceiverServer = null;
  ThreadGroup threadGroup = null;
  long blockReportInterval;
  //disallow the sending of BR before instructed to do so
  long lastBlockReport = 0;
  boolean resetBlockReportTime = true;
  long initialBlockReportDelay = BLOCKREPORT_INITIAL_DELAY * 1000L;
  long lastHeartbeat = 0;
  long heartBeatInterval;
  private DataStorage storage = null;
  private HttpServer infoServer = null;
  DataNodeMetrics myMetrics;
  private InetSocketAddress nameNodeAddr;
  private InetSocketAddress nameNodeAddrForClient;
  private InetSocketAddress selfAddr;
  private static DataNode datanodeObject = null;
  private Thread dataNodeThread = null;
  String machineName;
  private static String dnThreadName;
  int socketTimeout;
  int socketWriteTimeout = 0;  
  boolean transferToAllowed = true;
  int writePacketSize = 0;
  boolean isBlockTokenEnabled;
  BlockTokenSecretManager blockTokenSecretManager;
  boolean isBlockTokenInitialized = false;
  
  public DataBlockScanner blockScanner = null;
  public Daemon blockScannerThread = null;
  
  /** Activated plug-ins. */
  private List<ServicePlugin> plugins;
  
  private static final Random R = new Random();
  
  // For InterDataNodeProtocol
  public Server ipcServer;

  private SecureResources secureResources = null;  
  
  //removed by xianyu
//  public DatanodeStat dnStat=new DatanodeStat();    //Tongxin,collect the stat of datanode

  //add by xianyu
  public ServernodeStator dnStator = new ServernodeStator(ServernodeRole.DATANODE);
  
  /**
   * Create the DataNode given a configuration and an array of dataDirs.
   * 'dataDirs' is where the blocks are stored.
   */
  DataNode(final Configuration conf, 
           final AbstractList<File> dataDirs) throws IOException {
    this(conf, dataDirs, null);
  }
  
  /**
   * Start a Datanode with specified server sockets for secure environments
   * where they are run with privileged ports and injected from a higher
   * level of capability
   */
  DataNode(final Configuration conf,
           final AbstractList<File> dataDirs, final SecureResources resources) throws IOException {  
    this(conf, dataDirs, (DatanodeProtocol)RPC.waitForProxy(DatanodeProtocol.class,
                       DatanodeProtocol.versionID,
                       NameNode.getServiceAddress(conf, true), 
                       conf), resources);
  }
  
  /**
   * Create the DataNode given a configuration, an array of dataDirs,
   * and a namenode proxy
   */
  DataNode(final Configuration conf, 
           final AbstractList<File> dataDirs,
           final DatanodeProtocol namenode, final SecureResources resources) throws IOException {
    super(conf);

    DataNode.setDataNode(this);
    
    try {
      startDataNode(conf, dataDirs, namenode, resources);
    } catch (IOException ie) {
      shutdown();
     throw ie;
   }
  }

  private void initConfig(Configuration conf) throws UnknownHostException {
    // use configured nameserver & interface to get local hostname
    if (conf.get(DFSConfigKeys.DFS_DATANODE_HOST_NAME_KEY) != null) {
      machineName = conf.get(DFSConfigKeys.DFS_DATANODE_HOST_NAME_KEY);   
    }
    if (machineName == null) {
      machineName = DNS.getDefaultHost(
                                     conf.get("dfs.datanode.dns.interface","default"),
                                     conf.get("dfs.datanode.dns.nameserver","default"));
    }
    this.nameNodeAddr = NameNode.getServiceAddress(conf, true);
    this.nameNodeAddrForClient = NameNode.getAddress(conf);
    
    this.socketTimeout =  conf.getInt(DFSConfigKeys.DFS_CLIENT_SOCKET_TIMEOUT_KEY,
                                      HdfsConstants.READ_TIMEOUT);
    this.socketWriteTimeout = conf.getInt("dfs.datanode.socket.write.timeout",
                                          HdfsConstants.WRITE_TIMEOUT);
    /* Based on results on different platforms, we might need set the default 
     * to false on some of them. */
    this.transferToAllowed = conf.getBoolean("dfs.datanode.transferTo.allowed", 
                                             true);
    this.writePacketSize = conf.getInt(DFSConfigKeys.DFS_CLIENT_WRITE_PACKET_SIZE_KEY, 
                                       DFSConfigKeys.DFS_CLIENT_WRITE_PACKET_SIZE_DEFAULT);

    this.blockReportInterval =
      conf.getLong("dfs.blockreport.intervalMsec", BLOCKREPORT_INTERVAL);
    this.initialBlockReportDelay = conf.getLong("dfs.blockreport.initialDelay",
                                            BLOCKREPORT_INITIAL_DELAY)* 1000L; 
    if (this.initialBlockReportDelay >= blockReportInterval) {
      this.initialBlockReportDelay = 0;
      LOG.info("dfs.blockreport.initialDelay is greater than " +
        "dfs.blockreport.intervalMsec." + " Setting initial delay to 0 msec:");
    }
    this.heartBeatInterval = conf.getLong("dfs.heartbeat.interval", HEARTBEAT_INTERVAL) * 1000L;
  }
  
  private void startInfoServer(Configuration conf) throws IOException {
    // create a servlet to serve full-file content
    InetSocketAddress infoSocAddr = DataNode.getInfoAddr(conf);
    String infoHost = infoSocAddr.getHostName();
    int tmpInfoPort = infoSocAddr.getPort();
    this.infoServer = (secureResources == null) 
       ? new HttpServer("datanode", infoHost, tmpInfoPort, tmpInfoPort == 0, 
           conf, new AccessControlList(conf.get(DFSConfigKeys.DFS_ADMIN, " ")))
       : new HttpServer("datanode", infoHost, tmpInfoPort, tmpInfoPort == 0,
           conf, new AccessControlList(conf.get(DFSConfigKeys.DFS_ADMIN, " ")),
           secureResources.getListener());
    if(LOG.isDebugEnabled()) {
      LOG.debug("Datanode listening on " + infoHost + ":" + tmpInfoPort);
    }
    if (conf.getBoolean("dfs.https.enable", false)) {
      boolean needClientAuth = conf.getBoolean(DFSConfigKeys.DFS_CLIENT_HTTPS_NEED_AUTH_KEY,
                                               DFSConfigKeys.DFS_CLIENT_HTTPS_NEED_AUTH_DEFAULT);
      InetSocketAddress secInfoSocAddr = NetUtils.createSocketAddr(conf.get(
          "dfs.datanode.https.address", infoHost + ":" + 0));
      Configuration sslConf = new HdfsConfiguration(false);
      sslConf.addResource(conf.get("dfs.https.server.keystore.resource",
          "ssl-server.xml"));
      this.infoServer.addSslListener(secInfoSocAddr, sslConf, needClientAuth);
      if(LOG.isDebugEnabled()) {
        LOG.debug("Datanode listening for SSL on " + secInfoSocAddr);
      }
    }
    this.infoServer.addInternalServlet(null, "/streamFile/*", StreamFile.class);
    this.infoServer.addInternalServlet(null, "/getFileChecksum/*",
        FileChecksumServlets.GetServlet.class);
    this.infoServer.setAttribute("datanode.blockScanner", blockScanner);
    this.infoServer.setAttribute(JspHelper.CURRENT_CONF, conf);
    this.infoServer.addServlet(null, "/blockScannerReport", 
                               DataBlockScanner.Servlet.class);
    
    //add by xianyu
    this.infoServer.addInternalServlet("monitor", "/monitor", 
    //		MonitorServlet.GetDatanodeLogServlet.class, false);
    		DatanodeMonitorServlet.class, false);
    
    this.infoServer.start();
    // adjust info port
    this.dnRegistration.setInfoPort(this.infoServer.getPort());
  }
  
  private void initFsDataSet(Configuration conf, AbstractList<File> dataDirs)
      throws IOException {
    // get version and id info from the name-node
    NamespaceInfo nsInfo = handshake();

    StartupOption startOpt = getStartupOption(conf);
    assert startOpt != null : "Startup option must be set.";
    

    boolean simulatedFSDataset = 
        conf.getBoolean("dfs.datanode.simulateddatastorage", false);
    if (simulatedFSDataset) {
        setNewStorageID(dnRegistration);
        dnRegistration.storageInfo.layoutVersion = FSConstants.LAYOUT_VERSION;
        dnRegistration.storageInfo.namespaceID = nsInfo.namespaceID;
        // it would have been better to pass storage as a parameter to
        // constructor below - need to augment ReflectionUtils used below.
        conf.set(DFSConfigKeys.DFS_DATANODE_STORAGEID_KEY, dnRegistration.getStorageID());
        try {
          //Equivalent of following (can't do because Simulated is in test dir)
          //  this.data = new SimulatedFSDataset(conf);
          this.data = (FSDatasetInterface) ReflectionUtils.newInstance(
              Class.forName("org.apache.hadoop.hdfs.server.datanode.SimulatedFSDataset"), conf);
        } catch (ClassNotFoundException e) {
          throw new IOException(StringUtils.stringifyException(e));
        }
    } else { // real storage
      // read storage info, lock data dirs and transition fs state if necessary
      storage.recoverTransitionRead(nsInfo, dataDirs, startOpt);
      // adjust
      this.dnRegistration.setStorageInfo(storage);
      // initialize data node internal structure
      this.data = new FSDataset(storage, conf);
    }
  }
  

  private void startPlugins(Configuration conf) {
    plugins = conf.getInstances("dfs.datanode.plugins", ServicePlugin.class);
    for (ServicePlugin p: plugins) {
      try {
        p.start(this);
        LOG.info("Started plug-in " + p);
      } catch (Throwable t) {
        LOG.warn("ServicePlugin " + p + " could not be started", t);
      }
    }
  }
  

  private void initIpcServer(Configuration conf) throws IOException {
    InetSocketAddress ipcAddr = NetUtils.createSocketAddr(
        conf.get("dfs.datanode.ipc.address"));
    ipcServer = RPC.getServer(DataNode.class, this, ipcAddr.getHostName(),
        ipcAddr.getPort(), conf.getInt("dfs.datanode.handler.count", 3), false,
        conf, blockTokenSecretManager);
    
    // set service-level authorization security policy
    if (conf.getBoolean(
        CommonConfigurationKeys.HADOOP_SECURITY_AUTHORIZATION, false)) {
      ipcServer.refreshServiceAcl(conf, new HDFSPolicyProvider());
    }

    dnRegistration.setIpcPort(ipcServer.getListenerAddress().getPort());
    LOG.info("dnRegistration = " + dnRegistration);
  }
  

  private void initBlockScanner(Configuration conf) {
    String reason = null;
    if (conf.getInt("dfs.datanode.scan.period.hours", 0) < 0) {
      reason = "verification is turned off by configuration";
    } else if ( !(data instanceof FSDataset) ) {
      reason = "verifcation is supported only with FSDataset";
    } 
    if ( reason == null ) {
      blockScanner = new DataBlockScanner(this, (FSDataset)data, conf);
    } else {
      LOG.info("Periodic Block Verification is disabled because " +
               reason + ".");
    }
  }
  
  private void initDataXceiver(Configuration conf) throws IOException {
    // construct registration
    InetSocketAddress socAddr = DataNode.getStreamingAddr(conf);
    int tmpPort = socAddr.getPort();
    this.dnRegistration = new DatanodeRegistration(machineName + ":" + tmpPort);

    // find free port or use privileged port provided
    ServerSocket ss;
    if(secureResources == null) {
      ss = (socketWriteTimeout > 0) ? 
          ServerSocketChannel.open().socket() : new ServerSocket();
          Server.bind(ss, socAddr, 0);
    } else {
      ss = secureResources.getStreamingSocket();
    }
    ss.setReceiveBufferSize(DEFAULT_DATA_SOCKET_SIZE); 
    // adjust machine name with the actual port
    tmpPort = ss.getLocalPort();
    selfAddr = new InetSocketAddress(ss.getInetAddress().getHostAddress(),
                                     tmpPort);
    this.dnRegistration.setName(machineName + ":" + tmpPort);
    LOG.info("Opened info server at " + tmpPort);
      
    this.threadGroup = new ThreadGroup("dataXceiverServer");
    this.dataXceiverServer = new Daemon(threadGroup, 
        new DataXceiverServer(ss, conf, this));
    this.threadGroup.setDaemon(true); // auto destroy when empty
  }
  
  /**
   * This method starts the data node with the specified conf.
   * 
   * @param conf - the configuration
   *  if conf's CONFIG_PROPERTY_SIMULATED property is set
   *  then a simulated storage based data node is created.
   * 
   * @param dataDirs - only for a non-simulated storage data node
   * @throws IOException
   */
  void startDataNode(Configuration conf, 
                     AbstractList<File> dataDirs,
                     DatanodeProtocol namenode, SecureResources resources
                     ) throws IOException {
    if(UserGroupInformation.isSecurityEnabled() && resources == null)
      throw new RuntimeException("Cannot start secure cluster without " +
      "privileged resources.");

    this.secureResources = resources;
    this.namenode = namenode;
    storage = new DataStorage();
    
    initConfig(conf);
    registerMXBean();
    initDataXceiver(conf);
    initFsDataSet(conf, dataDirs);
    initBlockScanner(conf);
    startInfoServer(conf);
  
    myMetrics = new DataNodeMetrics(conf, dnRegistration.getName());
    // TODO check what code removed here

    initIpcServer(conf);
    startPlugins(conf);
    
    // BlockTokenSecretManager is created here, but it shouldn't be
    // used until it is initialized in register().
    this.blockTokenSecretManager = new BlockTokenSecretManager(false, 0, 0);
  }

  /**
   * Determine the http server's effective addr
   */
  public static InetSocketAddress getInfoAddr(Configuration conf) {
    return NetUtils.createSocketAddr(
        conf.get("dfs.datanode.http.address", "0.0.0.0:50075"));
  }
  
  private void registerMXBean() {
    // register MXBean
    MBeanServer mbs = ManagementFactory.getPlatformMBeanServer(); 
    try {
      ObjectName mxbeanName = new ObjectName("HadoopInfo:type=DataNodeInfo");
      mbs.registerMBean(this, mxbeanName);
    } catch ( javax.management.InstanceAlreadyExistsException iaee ) {
      // in unit tests, we may have multiple datanodes in the same JVM
      LOG.info("DataNode MXBean already registered");
    } catch ( javax.management.JMException e ) {
      LOG.warn("Failed to register DataNode MXBean", e);
    }
  }

  /**
   * Creates either NIO or regular depending on socketWriteTimeout.
   */
  protected Socket newSocket() throws IOException {
    return (socketWriteTimeout > 0) ? 
           SocketChannel.open().socket() : new Socket();                                   
  }
  
  private NamespaceInfo handshake() throws IOException {
    NamespaceInfo nsInfo = new NamespaceInfo();
    while (shouldRun) {
      try {
        nsInfo = namenode.versionRequest();
        break;
      } catch(SocketTimeoutException e) {  // namenode is busy
        LOG.info("Problem connecting to server: " + getNameNodeAddr());
        try {
          Thread.sleep(1000);
        } catch (InterruptedException ie) {}
      }
    }
    String errorMsg = null;
    // verify build version
    if( ! nsInfo.getBuildVersion().equals( Storage.getBuildVersion() )) {
      errorMsg = "Incompatible build versions: namenode BV = " 
        + nsInfo.getBuildVersion() + "; datanode BV = "
        + Storage.getBuildVersion();
      LOG.fatal( errorMsg );
      try {
        namenode.errorReport( dnRegistration,
                              DatanodeProtocol.NOTIFY, errorMsg );
      } catch( SocketTimeoutException e ) {  // namenode is busy
        LOG.info("Problem connecting to server: " + getNameNodeAddr());
      }
      throw new IOException( errorMsg );
    }
    assert FSConstants.LAYOUT_VERSION == nsInfo.getLayoutVersion() :
      "Data-node and name-node layout versions must be the same."
      + "Expected: "+ FSConstants.LAYOUT_VERSION + " actual "+ nsInfo.getLayoutVersion();
    return nsInfo;
  }

  private static void setDataNode(DataNode node) {
    datanodeObject = node;
  }

  /** Return the DataNode object
   * 
   */
  public static DataNode getDataNode() {
    return datanodeObject;
  } 

  public static InterDatanodeProtocol createInterDataNodeProtocolProxy(
      DatanodeID datanodeid, final Configuration conf, final int socketTimeout)
    throws IOException {
    final InetSocketAddress addr = NetUtils.createSocketAddr(
        datanodeid.getHost() + ":" + datanodeid.getIpcPort());
    if (InterDatanodeProtocol.LOG.isDebugEnabled()) {
      InterDatanodeProtocol.LOG.debug("InterDatanodeProtocol addr=" + addr);
    }
    UserGroupInformation loginUgi = UserGroupInformation.getLoginUser();
    try {
      return loginUgi
          .doAs(new PrivilegedExceptionAction<InterDatanodeProtocol>() {
            public InterDatanodeProtocol run() throws IOException {
              return (InterDatanodeProtocol) RPC.getProxy(
                  InterDatanodeProtocol.class, InterDatanodeProtocol.versionID,
                  addr, UserGroupInformation.getCurrentUser(), conf,
                  NetUtils.getDefaultSocketFactory(conf), socketTimeout);
            }
          });
    } catch (InterruptedException ie) {
      throw new IOException(ie.getMessage());
    }
  }

  public InetSocketAddress getNameNodeAddr() {
    return nameNodeAddr;
  }
  
  public InetSocketAddress getNameNodeAddrForClient() {
    return nameNodeAddrForClient;
  }
  
  public InetSocketAddress getSelfAddr() {
    return selfAddr;
  }
    
  DataNodeMetrics getMetrics() {
    return myMetrics;
  }
  
  /** Return DatanodeRegistration */
  public DatanodeRegistration getDatanodeRegistration() {
    return dnRegistration;
  }

  public static void setNewStorageID(DatanodeRegistration dnReg) {
    /* Return 
     * "DS-randInt-ipaddr-currentTimeMillis"
     * It is considered extermely rare for all these numbers to match
     * on a different machine accidentally for the following 
     * a) SecureRandom(INT_MAX) is pretty much random (1 in 2 billion), and
     * b) Good chance ip address would be different, and
     * c) Even on the same machine, Datanode is designed to use different ports.
     * d) Good chance that these are started at different times.
     * For a confict to occur all the 4 above have to match!.
     * The format of this string can be changed anytime in future without
     * affecting its functionality.
     */
    String ip = "unknownIP";
    try {
      ip = DNS.getDefaultIP("default");
    } catch (UnknownHostException ignored) {
      LOG.warn("Could not find ip address of \"default\" inteface.");
    }
    
    int rand = 0;
    try {
      rand = SecureRandom.getInstance("SHA1PRNG").nextInt(Integer.MAX_VALUE);
    } catch (NoSuchAlgorithmException e) {
      LOG.warn("Could not use SecureRandom");
      rand = R.nextInt(Integer.MAX_VALUE);
    }
    dnReg.storageID = "DS-" + rand + "-"+ ip + "-" + dnReg.getPort() + "-" + 
                      System.currentTimeMillis();
  }
  /**
   * Register datanode
   * <p>
   * The datanode needs to register with the namenode on startup in order
   * 1) to report which storage it is serving now and 
   * 2) to receive a registrationID 
   * issued by the namenode to recognize registered datanodes.
   * 
   * @see FSNamesystem#registerDatanode(DatanodeRegistration)
   * @throws IOException
   */
  private void register() throws IOException {
    if (dnRegistration.getStorageID().equals("")) {
      setNewStorageID(dnRegistration);
    }
    while(shouldRun) {
      try {
        // reset name to machineName. Mainly for web interface.
        dnRegistration.name = machineName + ":" + dnRegistration.getPort();
        dnRegistration = namenode.registerDatanode(dnRegistration);
        break;
      } catch(SocketTimeoutException e) {  // namenode is busy
        LOG.info("Problem connecting to server: " + getNameNodeAddr());
        try {
          Thread.sleep(1000);
        } catch (InterruptedException ie) {}
      }
    }
    assert ("".equals(storage.getStorageID()) 
            && !"".equals(dnRegistration.getStorageID()))
            || storage.getStorageID().equals(dnRegistration.getStorageID()) :
            "New storageID can be assigned only if data-node is not formatted";
    if (storage.getStorageID().equals("")) {
      storage.setStorageID(dnRegistration.getStorageID());
      storage.writeAll();
      LOG.info("New storage id " + dnRegistration.getStorageID()
          + " is assigned to data-node " + dnRegistration.getName());
    }
    if(! storage.getStorageID().equals(dnRegistration.getStorageID())) {
      throw new IOException("Inconsistent storage IDs. Name-node returned "
          + dnRegistration.getStorageID() 
          + ". Expecting " + storage.getStorageID());
    }
    
    if (!isBlockTokenInitialized) {
      /* first time registering with NN */
      ExportedBlockKeys keys = dnRegistration.exportedKeys;
      this.isBlockTokenEnabled = keys.isBlockTokenEnabled();
      if (isBlockTokenEnabled) {
        long blockKeyUpdateInterval = keys.getKeyUpdateInterval();
        long blockTokenLifetime = keys.getTokenLifetime();
        LOG.info("Block token params received from NN: keyUpdateInterval="
            + blockKeyUpdateInterval / (60 * 1000) + " min(s), tokenLifetime="
            + blockTokenLifetime / (60 * 1000) + " min(s)");
        blockTokenSecretManager.setTokenLifetime(blockTokenLifetime);
      }
      isBlockTokenInitialized = true;
    }

    if (isBlockTokenEnabled) {
      blockTokenSecretManager.setKeys(dnRegistration.exportedKeys);
      dnRegistration.exportedKeys = ExportedBlockKeys.DUMMY_KEYS;
    }

    // random short delay - helps scatter the BR from all DNs
    scheduleBlockReport(initialBlockReportDelay);
  }

  /**
   * Shut down this instance of the datanode.
   * Returns only after shutdown is complete.
   * This method can only be called by the offerService thread.
   * Otherwise, deadlock might occur.
   */
  public void shutdown() {
    if (plugins != null) {
      for (ServicePlugin p : plugins) {
        try {
          p.stop();
          LOG.info("Stopped plug-in " + p);
        } catch (Throwable t) {
          LOG.warn("ServicePlugin " + p + " could not be stopped", t);
        }
      }
    }
    
    if (infoServer != null) {
      try {
        infoServer.stop();
      } catch (Exception e) {
        LOG.warn("Exception shutting down DataNode", e);
      }
    }
    if (ipcServer != null) {
      ipcServer.stop();
    }
    this.shouldRun = false;
    if (dataXceiverServer != null) {
      ((DataXceiverServer) this.dataXceiverServer.getRunnable()).kill();
      this.dataXceiverServer.interrupt();

      // wait for all data receiver threads to exit
      if (this.threadGroup != null) {
        int sleepMs = 2;
        while (true) {
          this.threadGroup.interrupt();
          LOG.info("Waiting for threadgroup to exit, active threads is " +
                   this.threadGroup.activeCount());
          if (this.threadGroup.activeCount() == 0) {
            break;
          }
          try {
            Thread.sleep(sleepMs);
          } catch (InterruptedException e) {}
          sleepMs = sleepMs * 3 / 2; // exponential backoff
          if (sleepMs > 1000) {
            sleepMs = 1000;
          }
        }
      }
      // wait for dataXceiveServer to terminate
      try {
        this.dataXceiverServer.join();
      } catch (InterruptedException ie) {
      }
    }
    
    RPC.stopProxy(namenode); // stop the RPC threads
    
    if(upgradeManager != null)
      upgradeManager.shutdownUpgrade();
    if (blockScannerThread != null) { 
      blockScannerThread.interrupt();
      try {
        blockScannerThread.join(3600000L); // wait for at most 1 hour
      } catch (InterruptedException ie) {
      }
    }
    if (storage != null) {
      try {
        this.storage.unlockAll();
      } catch (IOException ie) {
        LOG.warn("Exception when unlocking storage: " + ie, ie);
      }
    }
    if (dataNodeThread != null) {
      dataNodeThread.interrupt();
      try {
        dataNodeThread.join();
      } catch (InterruptedException ie) {
      }
    }
    if (data != null) {
      data.shutdown();
    }
    if (myMetrics != null) {
      myMetrics.shutdown();
    }
  }
  
  
  /** Check if there is no space in disk 
   *  @param e that caused this checkDiskError call
   **/
  protected void checkDiskError(Exception e ) throws IOException {
    
    LOG.warn("checkDiskError: exception: ", e);
    
    if (e.getMessage() != null &&
        e.getMessage().startsWith("No space left on device")) {
      throw new DiskOutOfSpaceException("No space left on device");
    } else {
      checkDiskError();
    }
  }
  
  /**
   *  Check if there is a disk failure and if so, handle the error
   *
   **/
  protected void checkDiskError( ) {
    try {
      data.checkDataDir();
    } catch (DiskErrorException de) {
      handleDiskError(de.getMessage());
    }
  }
  
  private void handleDiskError(String errMsgr) {
    final boolean hasEnoughResources = data.hasEnoughResource();
    LOG.warn("DataNode.handleDiskError: Keep Running: " + hasEnoughResources);

    // If we have enough active valid volumes then we do not want to 
    // shutdown the DN completely.
    int dpError = hasEnoughResources ? DatanodeProtocol.DISK_ERROR  
                                     : DatanodeProtocol.FATAL_DISK_ERROR;  

    myMetrics.volumeFailures.inc(1);
    try {
      namenode.errorReport(dnRegistration, dpError, errMsgr);
    } catch (IOException e) {
      LOG.warn("Error reporting disk failure to NameNode: " + 
          StringUtils.stringifyException(e));
    }
    
    if (hasEnoughResources) {
      scheduleBlockReport(0);
      return; // do not shutdown
    }
    
    LOG.warn("DataNode is shutting down: " + errMsgr);
    shouldRun = false; 
  }
    
  /** Number of concurrent xceivers per node. */
  int getXceiverCount() {
    return threadGroup == null ? 0 : threadGroup.activeCount();
  }
    
  /**
   * Main loop for the DataNode.  Runs until shutdown,
   * forever calling remote NameNode functions.
   */
  public void offerService() throws Exception {
     
    LOG.info("using BLOCKREPORT_INTERVAL of " + blockReportInterval + "msec" + 
       " Initial delay: " + initialBlockReportDelay + "msec");

    //
    // Now loop for a long time....
    //
    while (shouldRun) {
      try {
        long startTime = now();

        //
        // Every so often, send heartbeat or block-report
        //
        
        if (startTime - lastHeartbeat > heartBeatInterval) {
          //
          // All heartbeat messages include following info:
          // -- Datanode name
          // -- data transfer port
          // -- Total capacity
          // -- Bytes remaining
          //
          ////******************* add by xianyu *************************/
          // -- cpu status: cur-rate, avg-rate, max-rate, min-rate.
          // -- mem status: cur-rate, avg-rate, max-rate, min-rate.
          // -- net status: []{name, bandwidth, duplex, cur-rate, avg-rate, max-rate, min-rate}
          // -- io status: []{cur-rate, avg-rate, max-rate, min-rate}
          ////***********************************************************/
          //
          lastHeartbeat = startTime;

       //removed by xianyu
//	   collectDatanodeStat();                 //tongxin added
//	    LOG.info("\r\n heartbeat :" +"\r\n  cpu: "+dnStat.getCpuUsed() +"\r\n  memory: "+dnStat.getMemUsed() 
//		+"\r\n  io: "+dnStat.getIoUsed());	
          DatanodeCommand[] cmds = namenode.sendHeartbeat(dnRegistration,
                                                       data.getCapacity(),
                                                       data.getDfsUsed(),
                                                       data.getRemaining(),
                                       /**** add by xianyu ****/
                                       dnStator.getCPUStatus(), 
                                       dnStator.getMEMStatus(), 
                                       dnStator.getNETStatus(), 
                                       dnStator.getIOStatus(), 
                                       /***********************/
                                                       //removed by xianyu
                                                       /*
 													   (long)(dnStat.getCpuUsed()*100),
                                                       (long)(dnStat.getMemUsed()*100),   // ww added
                                                       (long)(dnStat.getIoUsed()*100),
                                                       */
                                                       xmitsInProgress.get(),
                                                       getXceiverCount(),
                                                       data.getNumFailedVolumes());
          myMetrics.heartbeats.inc(now() - startTime);
          if (!processCommand(cmds))
            continue;
        }
            
        reportReceivedBlocks();

        DatanodeCommand cmd = blockReport();
        processCommand(cmd);

        // start block scanner
        if (blockScanner != null && blockScannerThread == null &&
            upgradeManager.isUpgradeCompleted()) {
          LOG.info("Starting Periodic block scanner.");
          blockScannerThread = new Daemon(blockScanner);
          blockScannerThread.start();
        }
            
        //
        // There is no work to do;  sleep until hearbeat timer elapses, 
        // or work arrives, and then iterate again.
        //
        long waitTime = heartBeatInterval - (System.currentTimeMillis() - lastHeartbeat);
        synchronized(receivedBlockList) {
          if (waitTime > 0 && receivedBlockList.size() == 0) {
            try {
              receivedBlockList.wait(waitTime);
            } catch (InterruptedException ie) {
            }
          }
        } // synchronized
      } catch(RemoteException re) {
        String reClass = re.getClassName();
        if (UnregisteredNodeException.class.getName().equals(reClass) ||
            DisallowedDatanodeException.class.getName().equals(reClass) ||
            IncorrectVersionException.class.getName().equals(reClass)) {
          LOG.warn("DataNode is shutting down: " + 
                   StringUtils.stringifyException(re));
          shutdown();
          return;
        }
        LOG.warn(StringUtils.stringifyException(re));
        try {
          long sleepTime = Math.min(1000, heartBeatInterval);
          Thread.sleep(sleepTime);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
        }
      } catch (IOException e) {
        LOG.warn(StringUtils.stringifyException(e));
      }
    } // while (shouldRun)
  } // offerService

  /**
   * Process an array of datanode commands
   * 
   * @param cmds an array of datanode commands
   * @return true if further processing may be required or false otherwise. 
   */
  private boolean processCommand(DatanodeCommand[] cmds) {
    if (cmds != null) {
      for (DatanodeCommand cmd : cmds) {
        try {
          if (processCommand(cmd) == false) {
            return false;
          }
        } catch (IOException ioe) {
          LOG.warn("Error processing datanode Command", ioe);
        }
      }
    }
    return true;
  }
  
    /**
     * 
     * @param cmd
     * @return true if further processing may be required or false otherwise. 
     * @throws IOException
     */
  private boolean processCommand(DatanodeCommand cmd) throws IOException {
    if (cmd == null)
      return true;
   
    final BlockCommand bcmd = cmd instanceof BlockCommand? (BlockCommand)cmd: null;

    switch(cmd.getAction()) {
    case DatanodeProtocol.DNA_TRANSFER:
      // Send a copy of a block to another datanode
      transferBlocks(bcmd.getBlocks(), bcmd.getTargets());
      myMetrics.blocksReplicated.inc(bcmd.getBlocks().length);
      break;
    case DatanodeProtocol.DNA_INVALIDATE:
      //
      // Some local block(s) are obsolete and can be 
      // safely garbage-collected.
      //
      Block toDelete[] = bcmd.getBlocks();
      try {
        if (blockScanner != null) {
          blockScanner.deleteBlocks(toDelete);
        }
        data.invalidate(toDelete);
      } catch(IOException e) {
        checkDiskError();
        throw e;
      }
      myMetrics.blocksRemoved.inc(toDelete.length);
      break;
    case DatanodeProtocol.DNA_SHUTDOWN:
      // shut down the data node
      this.shutdown();
      return false;
    case DatanodeProtocol.DNA_REGISTER:
      // namenode requested a registration - at start or if NN lost contact
      LOG.info("DatanodeCommand action: DNA_REGISTER");
      if (shouldRun) {
        register();
      }
      break;
    case DatanodeProtocol.DNA_FINALIZE:
      storage.finalizeUpgrade();
      break;
    case UpgradeCommand.UC_ACTION_START_UPGRADE:
      // start distributed upgrade here
      processDistributedUpgradeCommand((UpgradeCommand)cmd);
      break;
    case DatanodeProtocol.DNA_RECOVERBLOCK:
      recoverBlocks(((BlockRecoveryCommand)cmd).getRecoveringBlocks());
      break;
    case DatanodeProtocol.DNA_ACCESSKEYUPDATE:
      LOG.info("DatanodeCommand action: DNA_ACCESSKEYUPDATE");
      if (isBlockTokenEnabled) {
        blockTokenSecretManager.setKeys(((KeyUpdateCommand) cmd).getExportedKeys());
      }
      break;
    case DatanodeProtocol.DNA_CUMULUS_RECOVERY:
    	LOG.info("DNA_CUMULUS_RECOVERY");
    	CumulusRecoveryCommand ccmdCommand = (CumulusRecoveryCommand)cmd;
    	new Thread(new CumulusRecovery(ccmdCommand.getLostColumn(), ccmdCommand.getLocatedBlks(), ccmdCommand.getMatrix())).start();
    	break;
	// seq RCR_DN_EXECMD.1 1
	// added by ds at 2014-4-24
	case DatanodeProtocol.DNA_RC_RECOVERY:
	{
		LOG.info("DNA_RC_RECOVERY" + " ------dsLog ");
		RCRecoveryCommand rcRecoveryCmd = (RCRecoveryCommand) cmd;
		RCRecoveryThreadTarget recoveryTarget = new RCRecoveryThreadTarget(rcRecoveryCmd.getLostRow(),
				rcRecoveryCmd.getLocatedBlks(), rcRecoveryCmd.getMatrix());
		Thread rcRecoveryThread = new Thread(recoveryTarget);
		rcRecoveryThread.start();
		break;
	}
    default:
      LOG.warn("Unknown DatanodeCommand action: " + cmd.getAction());
    }
    return true;
  }

  // Distributed upgrade manager
  UpgradeManagerDatanode upgradeManager = new UpgradeManagerDatanode(this);

  private void processDistributedUpgradeCommand(UpgradeCommand comm
                                               ) throws IOException {
    assert upgradeManager != null : "DataNode.upgradeManager is null.";
    upgradeManager.processUpgradeCommand(comm);
  }

  /**
   * Report received blocks and delete hints to the Namenode
   * @throws IOException
   */
  private void reportReceivedBlocks() throws IOException {
    //check if there are newly received blocks
    Block [] blockArray=null;
    String [] delHintArray=null;
    synchronized(receivedBlockList) {
      synchronized(delHints){
        int numBlocks = receivedBlockList.size();
        if (numBlocks > 0) {
          if(numBlocks!=delHints.size()) {
            LOG.warn("Panic: receiveBlockList and delHints are not of the same length" );
          }
          //
          // Send newly-received blockids to namenode
          //
          blockArray = receivedBlockList.toArray(new Block[numBlocks]);
          delHintArray = delHints.toArray(new String[numBlocks]);
        }
      }
    }
    if (blockArray != null) {
      if(delHintArray == null || delHintArray.length != blockArray.length ) {
        LOG.warn("Panic: block array & delHintArray are not the same" );
      }
      namenode.blockReceived(dnRegistration, blockArray, delHintArray);
      synchronized(receivedBlockList) {
        synchronized(delHints){
          for(int i=0; i<blockArray.length; i++) {
            receivedBlockList.remove(blockArray[i]);
            delHints.remove(delHintArray[i]);
          }
        }
      }
    }
  }

  /**
   * Report the list blocks to the Namenode
   * @throws IOException
   */
  private DatanodeCommand blockReport() throws IOException {
    // send block report
    DatanodeCommand cmd = null;
    long startTime = now();
    if (startTime - lastBlockReport > blockReportInterval) {
      //
      // Send latest block report if timer has expired.
      // Get back a list of local block(s) that are obsolete
      // and can be safely GC'ed.
      //
      long brStartTime = now();
      BlockListAsLongs bReport = data.getBlockReport();

      cmd = namenode.blockReport(dnRegistration, bReport.getBlockListAsLongs());
      long brTime = now() - brStartTime;
      myMetrics.blockReports.inc(brTime);
      LOG.info("BlockReport of " + bReport.getNumberOfBlocks() +
          " blocks got processed in " + brTime + " msecs");
      //
      // If we have sent the first block report, then wait a random
      // time before we start the periodic block reports.
      //
      if (resetBlockReportTime) {
        lastBlockReport = startTime - R.nextInt((int)(blockReportInterval));
        resetBlockReportTime = false;
      } else {
        /* say the last block report was at 8:20:14. The current report
         * should have started around 9:20:14 (default 1 hour interval).
         * If current time is :
         *   1) normal like 9:20:18, next report should be at 10:20:14
         *   2) unexpected like 11:35:43, next report should be at 12:20:14
         */
        lastBlockReport += (now() - lastBlockReport) /
                           blockReportInterval * blockReportInterval;
      }
    }
    return cmd;
  }

  /**
   * Start distributed upgrade if it should be initiated by the data-node.
   */
  private void startDistributedUpgradeIfNeeded() throws IOException {
    UpgradeManagerDatanode um = DataNode.getDataNode().upgradeManager;
    assert um != null : "DataNode.upgradeManager is null.";
    if(!um.getUpgradeState())
      return;
    um.setUpgradeState(false, um.getUpgradeVersion());
    um.startUpgrade();
    return;
  }

  private void transferBlock( Block block, 
                              DatanodeInfo xferTargets[] 
                              ) throws IOException {
    if (!data.isValidBlock(block)) {
      // block does not exist or is under-construction
      String errStr = "Can't send invalid block " + block;
      LOG.info(errStr);
      namenode.errorReport(dnRegistration, 
                           DatanodeProtocol.INVALID_BLOCK, 
                           errStr);
      return;
    }

    // Check if NN recorded length matches on-disk length 
    long onDiskLength = data.getLength(block);
    if (block.getNumBytes() > onDiskLength) {
      // Shorter on-disk len indicates corruption so report NN the corrupt block
      namenode.reportBadBlocks(new LocatedBlock[]{
          new LocatedBlock(block, new DatanodeInfo[] {
              new DatanodeInfo(dnRegistration)})});
      LOG.info("Can't replicate block " + block
          + " because on-disk length " + onDiskLength 
          + " is shorter than NameNode recorded length " + block.getNumBytes());
      return;
    }
    
    int numTargets = xferTargets.length;
    if (numTargets > 0) {
      if (LOG.isInfoEnabled()) {
        StringBuilder xfersBuilder = new StringBuilder();
        for (int i = 0; i < numTargets; i++) {
          xfersBuilder.append(xferTargets[i].getName());
          xfersBuilder.append(" ");
        }
        LOG.info(dnRegistration + " Starting thread to transfer block " + 
                 block + " to " + xfersBuilder);                       
      }

      new Daemon(new DataTransfer(xferTargets, block, this)).start();
    }
  }

  private void transferBlocks( Block blocks[], 
                               DatanodeInfo xferTargets[][] 
                               ) {
    for (int i = 0; i < blocks.length; i++) {
      try {
        transferBlock(blocks[i], xferTargets[i]);
      } catch (IOException ie) {
        LOG.warn("Failed to transfer block " + blocks[i], ie);
      }
    }
  }

  /*
   * Informing the name node could take a long long time! Should we wait
   * till namenode is informed before responding with success to the
   * client? For now we don't.
   */
  protected void notifyNamenodeReceivedBlock(Block block, String delHint) {
    if(block==null || delHint==null) {
      throw new IllegalArgumentException(block==null?"Block is null":"delHint is null");
    }
    synchronized (receivedBlockList) {
      synchronized (delHints) {
        receivedBlockList.add(block);
        delHints.add(delHint);
        receivedBlockList.notifyAll();
      }
    }
  }

  


  /* ********************************************************************
  Protocol when a client reads data from Datanode (Cur Ver: 9):
  
  Client's Request :
  =================
   
     Processed in DataXceiver:
     +----------------------------------------------+
     | Common Header   | 1 byte OP == OP_READ_BLOCK |
     +----------------------------------------------+
     
     Processed in readBlock() :
     +-------------------------------------------------------------------------+
     | 8 byte Block ID | 8 byte genstamp | 8 byte start offset | 8 byte length |
     +-------------------------------------------------------------------------+
     |   vInt length   |  <DFSClient id> |
     +-----------------------------------+
     
     Client sends optional response only at the end of receiving data.
       
  DataNode Response :
  ===================
   
    In readBlock() :
    If there is an error while initializing BlockSender :
       +---------------------------+
       | 2 byte OP_STATUS_ERROR    | and connection will be closed.
       +---------------------------+
    Otherwise
       +---------------------------+
       | 2 byte OP_STATUS_SUCCESS  |
       +---------------------------+
       
    Actual data, sent by BlockSender.sendBlock() :
    
      ChecksumHeader :
      +--------------------------------------------------+
      | 1 byte CHECKSUM_TYPE | 4 byte BYTES_PER_CHECKSUM |
      +--------------------------------------------------+
      Followed by actual data in the form of PACKETS: 
      +------------------------------------+
      | Sequence of data PACKETs ....      |
      +------------------------------------+
    
    A "PACKET" is defined further below.
    
    The client reads data until it receives a packet with 
    "LastPacketInBlock" set to true or with a zero length. It then replies
    to DataNode with one of the status codes:
    - CHECKSUM_OK:    All the chunk checksums have been verified
    - SUCCESS:        Data received; checksums not verified
    - ERROR_CHECKSUM: (Currently not used) Detected invalid checksums

      +---------------+
      | 2 byte Status |
      +---------------+
    
    The DataNode expects all well behaved clients to send the 2 byte
    status code. And if the the client doesn't, the DN will close the
    connection. So the status code is optional in the sense that it
    does not affect the correctness of the data. (And the client can
    always reconnect.)
    
    PACKET : Contains a packet header, checksum and data. Amount of data
    ======== carried is set by BUFFER_SIZE.
    
      +-----------------------------------------------------+
      | 4 byte packet length (excluding packet header)      |
      +-----------------------------------------------------+
      | 8 byte offset in the block | 8 byte sequence number |
      +-----------------------------------------------------+
      | 1 byte isLastPacketInBlock                          |
      +-----------------------------------------------------+
      | 4 byte Length of actual data                        |
      +-----------------------------------------------------+
      | x byte checksum data. x is defined below            |
      +-----------------------------------------------------+
      | actual data ......                                  |
      +-----------------------------------------------------+
      
      x = (length of data + BYTE_PER_CHECKSUM - 1)/BYTES_PER_CHECKSUM *
          CHECKSUM_SIZE
          
      CHECKSUM_SIZE depends on CHECKSUM_TYPE (usually, 4 for CRC32)
      
      The above packet format is used while writing data to DFS also.
      Not all the fields might be used while reading.
    
   ************************************************************************ */

  /**
   * Used for transferring a block of data.  This class
   * sends a piece of data to another DataNode.
   */
  class DataTransfer implements Runnable {
    DatanodeInfo targets[];
    Block b;
    DataNode datanode;

    /**
     * Connect to the first item in the target list.  Pass along the 
     * entire target list, the block, and the data.
     */
    public DataTransfer(DatanodeInfo targets[], Block b, DataNode datanode) throws IOException {
      this.targets = targets;
      this.b = b;
      this.datanode = datanode;
    }

    /**
     * Do the deed, write the bytes
     */
    public void run() {
      xmitsInProgress.getAndIncrement();
      Socket sock = null;
      DataOutputStream out = null;
      BlockSender blockSender = null;
      
      try {
        InetSocketAddress curTarget = 
          NetUtils.createSocketAddr(targets[0].getName());
        sock = newSocket();
        NetUtils.connect(sock, curTarget, socketTimeout);
        sock.setSoTimeout(targets.length * socketTimeout);

        long writeTimeout = socketWriteTimeout + 
                            HdfsConstants.WRITE_TIMEOUT_EXTENSION * (targets.length-1);
        OutputStream baseStream = NetUtils.getOutputStream(sock, writeTimeout);
        out = new DataOutputStream(new BufferedOutputStream(baseStream, 
                                                            SMALL_BUFFER_SIZE));

        blockSender = new BlockSender(b, 0, b.getNumBytes(), 
            false, false, false, datanode);
        DatanodeInfo srcNode = new DatanodeInfo(dnRegistration);

        //
        // Header info
        //
        Token<BlockTokenIdentifier> accessToken = BlockTokenSecretManager.DUMMY_TOKEN;
        if (isBlockTokenEnabled) {
          accessToken = blockTokenSecretManager.generateToken(null, b,
          EnumSet.of(BlockTokenSecretManager.AccessMode.WRITE));
        }
        DataTransferProtocol.Sender.opWriteBlock(out,
            b, 0, BlockConstructionStage.PIPELINE_SETUP_CREATE, 0, 0, 0, "",
            srcNode, targets, accessToken);

        // send data & checksum
        blockSender.sendBlock(out, baseStream, null);

        // no response necessary
        LOG.info(dnRegistration + ":Transmitted block " + b + " to " + curTarget);

      } catch (IOException ie) {
        LOG.warn(dnRegistration + ":Failed to transfer " + b + " to " + targets[0].getName()
            + " got " + StringUtils.stringifyException(ie));
        // check if there are any disk problem
        datanode.checkDiskError();
        
      } finally {
        xmitsInProgress.getAndDecrement();
        IOUtils.closeStream(blockSender);
        IOUtils.closeStream(out);
        IOUtils.closeSocket(sock);
      }
    }
  }
  
  /**
   * added by czl
   * cumulus lost recovery daemon
   */
  class CumulusRecovery implements Runnable{
	  LocatedBlock[] locatedBlks;
	  CodingMatrix matrix;
	  byte lostColumn;
	  
	  public CumulusRecovery(byte lostColumn, LocatedBlock[] locatedblks, CodingMatrix matrix){
		  this.lostColumn = lostColumn;
		  this.locatedBlks = locatedblks;
		  this.matrix = matrix;
		  LOG.info("constructor..............");
	  }

	  @Override
	  public void run() {
		// TODO Auto-generated method stub
		  LOG.info(lostColumn+"  ");
		  LOG.info("matrix size: " + matrix.getRow() + "  " + matrix.getColumn());
		  LOG.info(matrix.toString());
		  
		  short[][] smatrix = new short[matrix.getRow()][matrix.getRow()];
		  short[] vector = new short[matrix.getRow()];
		  for (int i = 0; i < matrix.getRow(); i++) {
			  	int jj = 0;
				for (int j = 0; j < matrix.getColumn(); j++) {
					if (j == lostColumn) {
						vector[i] =  (short) (matrix.getElemAt(i, j)>=0 ? 
									matrix.getElemAt(i, j) 
									:(matrix.getElemAt(i, j)+256)); 
						continue;
					}
					if (jj < matrix.getRow()) {
						smatrix[i][jj++] = (short) (matrix.getElemAt(i, j)>=0 
											? matrix.getElemAt(i, j) 
											: (matrix.getElemAt(i, j)+256)); 
					}
					
				}
		  }
		  
		  for (int i = 0; i < smatrix.length; i++) {
			for (int j = 0; j < smatrix[i].length; j++) {
				LOG.info(smatrix[i][j]+ " ");
			}
		}
		  for (int i = 0; i < vector.length; i++) {
			LOG.info(vector[i]+" ");
		}
		  
		  short[][] inverse = RSCoderProtocol.getRSP().InitialInvertedCauchyMatrix(smatrix);
		  
		  for (int i = 0; i < inverse.length; i++) {
				for (int j = 0; j < inverse[i].length; j++) {
					LOG.info(inverse[i][j]+ " ");
				}
			}
		  
		  short[] coeffients = new short[vector.length];
		  for (int i = 0; i < inverse.length; i++) {
			  short tmp = 0;
			  for (int j = 0; j < inverse[i].length; j++) {
				  tmp ^= RSCoderProtocol.mult[inverse[i][j]][vector[j]];
		  }
			  coeffients[i] = tmp; 
		}
		  
		  for (int i = 0; i < coeffients.length; i++) {
			LOG.info(" "+coeffients[i]+"\n");
		}
		  
		  
		  for (int i = 0; i < locatedBlks.length; i++) {
			LOG.info(locatedBlks[i].toString());
		}
		BlockReader[] brs = new BlockReader[matrix.getRow()];
		  int j = 0;
		  for (int i = 0; i < locatedBlks.length && j < brs.length; i++) {
			if (i != lostColumn) {
				Socket socket = new Socket();			
				try {
					NetUtils.connect(socket, NetUtils.createSocketAddr(locatedBlks[i].getLocations()[0].getName()), 1000);
					brs[j] = BlockReader.newBlockReader(socket, "cumulus lost recover", locatedBlks[i].getBlock(), locatedBlks[i].getBlockToken(), 0, locatedBlks[i].getBlockSize(), 1024*1024*4);
					j++;
				} catch (IOException e) {
					// TODO Auto-generated catch block
					LOG.info(e.toString());
				}
			}
		}
		  
		LOG.info("block readers created.........");
		
		if (j < matrix.getRow()) {
			LOG.info("cumulus lost recover failed because no enough complete blocks");
		}
		else {
			
			ReplicaInPipelineInterface replicaInfo = null;
			FSDataset.BlockWriteStreams streams = null;
			OutputStream out = null;
			DataOutputStream checksumOut = null;
			DataChecksum checksum = null;
			long bytesCount = 0;
			byte[] lastChecksum = new byte[4];
			try {
				 checksum = DataChecksum.newDataChecksum(DataChecksum.CHECKSUM_CRC32, 512);
				 replicaInfo = data.createRbw(locatedBlks[lostColumn].getBlock());
				 streams = replicaInfo.createStreams(true, 512, 4);//need to reconsider
				 out = streams.dataOut;
				 checksumOut = new DataOutputStream(new BufferedOutputStream(
                         									streams.checksumOut, 
                         									SMALL_BUFFER_SIZE));
				 BlockMetadataHeader.writeHeader(checksumOut, checksum);
				 
			} catch (IOException e2) {
				// TODO Auto-generated catch block
				LOG.info(e2.toString());
			}
			
			LOG.info("streams created.....");
			
			
			int UnitLen = 512;
			byte[][] readBytes = new byte[matrix.getRow()][UnitLen];
			int[] readLen = new int[matrix.getRow()];
			int maxLen;
			
			while(true){
				
				maxLen = -1;
				for (int i = 0; i < brs.length; i++) {
					try {
						readLen[i] = brs[i].read(readBytes[i]);
						if (readLen[i] > maxLen) {
							maxLen = readLen[i];
						}
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} 
				}
				
				LOG.info("maxLen: "+maxLen);
				
				if (maxLen==-1) {
					
					break;
				}
				
				short[] input = new short[maxLen];
				short[] output = new short[maxLen];
				
				for (int i = 0; i < output.length; i++) {
					output[i] = 0; 
				}
				
				int k = matrix.getRow();
				for (int i = 0; i < k; i++){	
					  if(coeffients[i] != 0){
						  for (int p = 0; p < readLen[i]; p++)
						  {
							  if (readBytes[i][p] < 0)
								  input[p] = (short)(readBytes[i][p] + 256);
							  else 
								  input[p] = (short)readBytes[i][p];
						  }
						  for(int m = 0;m< readLen[i];m++)
						  {
							  	output[m] ^= RSCoderProtocol.mult[input[m]][coeffients[i]];
						  }
					
					  }				  
			  }
				
				byte[] outByte = new byte[output.length];
				for (int i = 0; i < outByte.length; i++) {
					outByte[i] = (byte)output[i]; 
				}
				try {
					bytesCount += maxLen;
					checksum.update(outByte, 0, maxLen);
					out.write(outByte, 0, maxLen);
					
					int integer = (int) checksum.getValue();
					lastChecksum[0] = (byte)((integer >>> 24) & 0xFF);
			       lastChecksum[1] = (byte)((integer >>> 16) & 0xFF);
			       lastChecksum[2] = (byte)((integer >>>  8) & 0xFF);
			       lastChecksum[3] = (byte)((integer >>>  0) & 0xFF);
				   
					checksumOut.write(lastChecksum);
					checksum.reset();
					
				} catch (IOException e) {
					// TODO Auto-generated catch block
					LOG.info(e.toString());
				}
				
			
			}
			
			LOG.info("close//////////");
			try {
				out.flush();
				out.close();
				checksumOut.flush();
				checksumOut.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				LOG.info(e.toString());
			}
			
			try {
				
				replicaInfo.setNumBytes(bytesCount);
				replicaInfo.setBytesAcked(bytesCount);
				LOG.info("finalize////////////");
				locatedBlks[lostColumn].getBlock().setNumBytes(bytesCount);
				data.finalizeBlock(locatedBlks[lostColumn].getBlock());
				myMetrics.blocksWritten.inc();
				closeBlock(locatedBlks[lostColumn].getBlock(), DataNode.EMPTY_DEL_HINT);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				LOG.info(e.toString());
			}
	
		}
		
	  }
	  
  }
  
//seq RCR_DN_MAIN.1 2
	/**
	 * rc(regenerating code)recovery thread target, changed on basis of CumulusRecovery. created at 2014-4-17. modified
	 * at 2014-4-24,2014-4-25.
	 * 
	 * @author ds
	 */
	public class RCRecoveryThreadTarget implements Runnable
	{
		byte failednodeRow; // the row index of failed node in V matrix
		LocatedBlock[] fileLocatedBlocks;// all LocatedBlocks of the file
		CodingMatrix matrix;//

		byte n; // how many storage nodes for a file
		byte k; // min amount of blocks to reconstruct file
		byte d; // how many helper nodes
		byte a; // how many blocks a node stores
		byte B; // how many cuts the file was cut

		byte[] helpernodeRows;// the row indexs of helper node in V matrix

		// added at 2014-4-24
		// object for waiting multi thread
		Object waitResult;
		// how many threads are getting result LocatedBlocks from helper nodes
		int waitResultCount;
		LocatedBlock[] resultLocatedBlocks;

		/**
		 * @param failednodeRow
		 *            the first of d lost blocks
		 * @param fileLocatedBlocks
		 *            blocks the file contains
		 * @param matrix
		 *            the RC matrix
		 */
		public RCRecoveryThreadTarget(byte failednodeRow, LocatedBlock[] fileLocatedBlocks, CodingMatrix matrix)
		{
			this.failednodeRow = failednodeRow;
			this.fileLocatedBlocks = fileLocatedBlocks;
			this.matrix = (RegeneratingCodeMatrix) matrix;

			this.n = (byte) this.matrix.getStoreFileNodesNum();
			this.d = (byte) this.matrix.getRecoveryNodesNum();
			this.a = this.d;
			this.B = (byte) this.matrix.getFileCutsNum();

			this.helpernodeRows = new byte[d];
			chooseHelperRows();

			this.waitResult = new Object();
			this.resultLocatedBlocks = new LocatedBlock[d];
		}

		@Override
		public void run()
		{
			long start = System.currentTimeMillis();
			// inform helper nodes to compute result blocks
			getResultBlocks();

			long getResultBlocksEnd = System.currentTimeMillis();
			LOG.info("getting the result blocks uses time : " + (getResultBlocksEnd - start) + "ms");
			// / get lost blocks
			Block[] lostBlocks = new Block[a];
			for (byte i = 0; i < a; i++)
			{
				int index = failednodeRow * a + i;
				lostBlocks[i] = fileLocatedBlocks[index].getBlock();
			}

			// / get repairInverseMatrix
			byte[][] repairInverseMatrix = getRepairInverseMatrix();

			// decode and recover
			decodeAndRecover(lostBlocks, resultLocatedBlocks, repairInverseMatrix);

			long decodeAndRecoverEnd = System.currentTimeMillis();
			LOG.info("decodeing and recovering the lost blocks uses time : "
					+ (decodeAndRecoverEnd - getResultBlocksEnd) + "ms");
			// inform helper nodes to delete temporary result block
			informHelpernodesToEnd(resultLocatedBlocks);
			long informHelpernodeToEndEnd = System.currentTimeMillis();

			LOG.info("informing helper node to end recovery uses time:"
					+ (informHelpernodeToEndEnd - decodeAndRecoverEnd) + "ms");
			long recoveryEnd = System.currentTimeMillis() - start;
			LOG.info("RCRecovery uses time :" + (recoveryEnd-start) + "ms");
		}

		private InterDatanodeProtocol getHelpernode(DatanodeInfo helperDatanodeInfo)
		{
			// get helper node
			InterDatanodeProtocol helpernode = null;
			DatanodeID helpernodeID = (DatanodeID) helperDatanodeInfo;
			try
			{
				if (dnRegistration.equals(helpernodeID))
				{
					helpernode = (InterDatanodeProtocol) this;
				}
				else
				{
					helpernode = (InterDatanodeProtocol) DataNode.createInterDataNodeProtocolProxy(helpernodeID,
							getConf(), socketTimeout);
				}
			}
			catch (IOException e)
			{
				LOG.warn("get helpernode InterDatanodeProtocol " + " EXCEPTION " + e.toString() + " ------dsLog ");
				return null;
			}
			return helpernode;
		}

		private void getResultBlocks()
		{
			// fill resultBlock list disordered, disorder is because multi threads
			waitResultCount = 0;
			for (byte helper = 0; helper < d; helper++)
			{
				// get helperDatanodeInfo
				DatanodeInfo helperDatanodeInfo;
				int repreBlockIndex = helpernodeRows[helper] * a;
				helperDatanodeInfo = fileLocatedBlocks[repreBlockIndex].getLocations()[0];

				// get helper node
				InterDatanodeProtocol helpernode = getHelpernode(helperDatanodeInfo);

				// get computeBlocks
				Block[] computeBlocks = new Block[a];
				for (byte i = 0; i < a; i++)
				{
					int index = helpernodeRows[helper] * a + i;
					computeBlocks[i] = fileLocatedBlocks[index].getBlock();

				}
				// get failednodeVector
				byte[] failednodeVector = getFailednodeVector();

				// call IPC method, fill resultBlock list disordered in multi threads
				FillResultLocatedBlocksThreadTarget threadTarget = new FillResultLocatedBlocksThreadTarget(helpernode,
						computeBlocks, failednodeVector, helperDatanodeInfo);
				Thread thread = new Thread(threadTarget);
				try
				{
					thread.start();
				}
				catch (RuntimeException e)
				{
					// need to refine
					LOG.warn("RCR thread has to be terminated " + " ------dsLog ");
					return;
				}
				synchronized (waitResult)
				{
					waitResultCount++;
					waitResult.notifyAll();
				}
			}
			// / waiting for result
			while (true)
			{
				synchronized (waitResult)
				{
					if (waitResultCount == 0)
					{
						break;
					}
					else
					{

						try
						{
							waitResult.wait();
						}
						catch (InterruptedException e)
						{
							LOG.warn("waiting for result" + " InterruptedException " + e.toString() + " ------dsLog ");
							LOG.info("RCR thread has to be terminated " + " ------dsLog ");
							return;
						}

					}
				}
			}
		}

		private void informHelpernodesToEnd(LocatedBlock[] resultLocatedBlocks)
		{
			for (byte helper = 0; helper < d; helper++)
			{
				// get helperDatanodeInfo
				DatanodeInfo helperDatanodeInfo;
				int repreBlockIndex = helpernodeRows[helper] * a;
				helperDatanodeInfo = fileLocatedBlocks[repreBlockIndex].getLocations()[0];

				// get helper node
				InterDatanodeProtocol helpernode = getHelpernode(helperDatanodeInfo);
				try
				{
					helpernode.endRCRecovery(resultLocatedBlocks[helper].getBlock());
				}
				catch (IOException e)
				{
					LOG.warn("inform helper endRCR i = " + helper + " InterruptedException " + e.toString()
							+ " ------dsLog ");
					LOG.info("RCR thread has to be terminated " + " ------dsLog ");
					return;
				}
			}
		}

		// added at 2014-4-24
		private class FillResultLocatedBlocksThreadTarget implements Runnable
		{

			InterDatanodeProtocol helper;
			Block[] computeBlocks;
			byte[] failednodeVector;
			DatanodeInfo helperDatanodeInfo;

			public FillResultLocatedBlocksThreadTarget(InterDatanodeProtocol helper, Block[] computeBlocks,
					byte[] failednodeVector, DatanodeInfo helperDatanodeInfo)
			{
				super();
				this.helper = helper;
				this.computeBlocks = computeBlocks;
				this.failednodeVector = failednodeVector;
				this.helperDatanodeInfo = helperDatanodeInfo;
			}

			@Override
			public void run()
			{
				LocatedBlock resultLocatedBlock = null;
				try
				{
					resultLocatedBlock = helper.beginRCRecovery(helperDatanodeInfo, computeBlocks, failednodeVector);
				}
				catch (IOException e)
				{
					LOG.warn(" get result thread" + " RuntimeException " + e.toString() + " ------dsLog ");
					throw new RuntimeException("get result tread running" + " EXCEPTION ", e);

				}
				synchronized (waitResult)
				{
					for (int i = 0; i < d; i++)
					{
						// get helperDatanodeInfo
						DatanodeInfo helperDatanodeInfo;
						int repreBlockIndex = helpernodeRows[i] * a;
						helperDatanodeInfo = fileLocatedBlocks[repreBlockIndex].getLocations()[0];
						if (helperDatanodeInfo.equals(resultLocatedBlock.getLocations()[0]))
						{
							resultLocatedBlocks[i] = resultLocatedBlock;
							break;
						}
					}
					waitResultCount--;
					waitResult.notifyAll();
				}

			}
		}

		// This method is abstracted to keep tidy in constructor
		private void chooseHelperRows()
		{// choose front d rows in V matrix who do not include lost row
			for (byte count = 0, row = 0; count < d && row < n; row++)// 6 : n
			// need to refine
			{
				if (failednodeRow != row)
				{
					helpernodeRows[count] = row;
					count++;
				}
			}
		}

		private byte[][] getRepairInverseMatrix()
		{
			// need to refine
			// get V matrix : n*a
			byte[][] v = this.matrix.getVandermondeMatrix();
			// get repair matrix in short : d*a
			short[][] repairMatrixInShort = new short[d][a];
			for (int helper = 0; helper < d; helper++)
			{
				int helpernodeRow = helpernodeRows[helper];
				for (int i = 0; i < a; i++)
				{
					byte element = v[helpernodeRow][i];
					repairMatrixInShort[helper][i] = (short) (element >= 0 ? element : element + 256);
				}
			}
			// get inverse of repair matrix in short: d*a
			short[][] repairInverseMatrixInShort = RSCoderProtocol.getRSP().InitialInvertedCauchyMatrix(
					repairMatrixInShort);

			// get inverse of repair matrix
			byte[][] repairInverseMatrix = new byte[d][a];
			for (int i = 0; i < repairInverseMatrix.length; i++)
			{
				for (int j = 0; j < repairInverseMatrix[i].length; j++)
				{
					// repairInverseMatrix[i][j] = (byte) repairMatrixInShort[i][j]; //2014-4-29
					repairInverseMatrix[i][j] = (byte) repairInverseMatrixInShort[i][j];

				}
			}
			return repairInverseMatrix;
		}

		private byte[] getFailednodeVector()
		{
			// get V matrix : n*a
			byte[][] v = this.matrix.getVandermondeMatrix();
			// get failednodeVector
			byte[] vectorFailednode = v[failednodeRow];

			return vectorFailednode;
		}

		private void decodeAndRecover(Block[] lostBloks, LocatedBlock[] resultLocatedBlocks,
				byte[][] repairInverseMatrix)
		{
			final int d = resultLocatedBlocks.length;
			final int a = d;
			final int unitSize = 512;

			// create BlockReaders
			BlockReader blockReaders[] = new BlockReader[d];
			for (int i = 0; i < d; i++)
			{
				try
				{
					// socket
					Socket socket = new Socket();
					SocketAddress address = NetUtils.createSocketAddr(resultLocatedBlocks[i].getLocations()[0]
							.getName());
					NetUtils.connect(socket, address, 1000);
					// create BlockReader
					blockReaders[i] = BlockReader.newBlockReader(socket, "cumulus lost recover", resultLocatedBlocks[i]
							.getBlock(), resultLocatedBlocks[i].getBlockToken(), 0, resultLocatedBlocks[i]
							.getBlockSize(), 1024 * 1024 * 4);
				}
				catch (IOException e)
				{

					LOG.warn("create BlockReader i=" + i + " EXCEPTION " + e.toString() + " ------dsLog ");
					return;
				}
			}

			// create new files and new checksum file for the lost blocks
			ReplicaInPipelineInterface[] replicaInfos = new ReplicaInPipelineInterface[a];
			for (int i = 0; i < a; i++)
			{
				try
				{
					replicaInfos[i] = data.createRbw(lostBloks[i]);
				}
				catch (IOException e)
				{
					LOG.warn("create new files i=" + i + " EXCEPTION " + e.toString() + " ------dsLog ");
					return;
				}
			}

			// create outs to blocks file and outs to checksum files
			OutputStream[] outs = new OutputStream[a];
			DataOutputStream[] checksumOuts = new DataOutputStream[a];
			for (int i = 0; i < a; i++)
			{
				try
				{
					FSDataset.BlockWriteStreams streams = null;
					// need to reconsider
					streams = replicaInfos[i].createStreams(true, unitSize, 4);
					outs[i] = streams.dataOut;
					checksumOuts[i] = new DataOutputStream(new BufferedOutputStream(streams.checksumOut,
							SMALL_BUFFER_SIZE));

				}
				catch (IOException e)
				{
					LOG.warn("create outs i=" + i + " EXCEPTION " + e.toString() + " ------dsLog ");
					return;
				}
			}

			// create chescksum
			DataChecksum checksum = null;
			checksum = DataChecksum.newDataChecksum(DataChecksum.CHECKSUM_CRC32, unitSize);

			// write header to checksumOuts
			for (int i = 0; i < a; i++)
			{
				try
				{
					// /write header to checksumOuts
					BlockMetadataHeader.writeHeader(checksumOuts[i], checksum);
				}
				catch (IOException e)
				{
					LOG.warn("write header to checksumOuts i=" + i + " EXCEPTION " + e.toString() + " ------dsLog ");
					return;
				}
			}

			long bytesCount = 0;
			while (true)
			{
				int maxLen = -1;
				// read bytes from ins to inBufs
				byte[][] inBufs = new byte[d][unitSize];
				for (int i = 0; i < d; i++)
				{
					try
					{
						int inLen = -1;
						inLen = blockReaders[i].read(inBufs[i]);
						if (inLen > maxLen)
						{
							maxLen = inLen;
						}
					}
					catch (IOException e)
					{
						LOG.warn("read bytes from ins to inBufs i=" + i + " EXCEPTION " + e.toString()
								+ " ------dsLog ");
						return;
					}
				}
				// judge last read how many bytes
				if (maxLen == -1)
				{
					break;
				}

				LOG.info("maxLen = " + maxLen + " ------dsLog ");
				// repairMatrixInverse left multiply inBufs, write results to
				// outBufs, more details see draft
				byte[][] outBufs = new byte[a][unitSize];
				for (int i = 0; i < d; i++)
				{
					for (int k = 0; k < unitSize; k++)
					{
						for (int j = 0; j < d; j++)
						{
							byte f1 = repairInverseMatrix[i][j];
							byte f2 = inBufs[j][k];
							outBufs[i][k] ^= RSCoderProtocol.getRSP().mult(f1, f2);
						}
					}
				}
				// write to outs
				for (int i = 0; i < a; i++)
				{
					try
					{
						outs[i].write(outBufs[i], 0, maxLen);
					}
					catch (IOException e)
					{
						LOG.warn("write to outs i=" + i + " EXCEPTION " + e.toString() + " ------dsLog ");
						return;
					}
				}

				// compute checksum and write results to checksunOuts
				for (int i = 0; i < a; i++)
				{
					checksum.update(outBufs[i], 0, maxLen);
					int integer = (int) checksum.getValue();
					byte[] checksumValueBuf = new byte[4];
					checksumValueBuf[0] = (byte) ((integer >>> 24) & 0xFF);
					checksumValueBuf[1] = (byte) ((integer >>> 16) & 0xFF);
					checksumValueBuf[2] = (byte) ((integer >>> 8) & 0xFF);
					checksumValueBuf[3] = (byte) ((integer >>> 0) & 0xFF);
					checksum.reset();
					// write to checksumOut
					try
					{
						checksumOuts[i].write(checksumValueBuf);
					}
					catch (IOException e)
					{
						LOG.warn("write to checksumOuts i=" + i + " EXCEPTION " + e.toString() + " ------dsLog ");
						return;
					}
				}

				// bytesCount update
				bytesCount += maxLen;
			}
			// close and flush
			for (int i = 0; i < d; i++)
			{
				try
				{
					outs[i].flush();
					checksumOuts[i].flush();
					outs[i].close();
					checksumOuts[i].close();
				}
				catch (IOException e)
				{
					LOG.warn("close and flush i = " + i + " EXCEPTION " + e.toString() + " ------dsLog ");
					return;
				}
			}

			// finalize and close replicas and blocks
			LOG.info("bytesCount: " + bytesCount + " ------dsLog ");

			for (int i = 0; i < a; i++)
			{
				try
				{
					replicaInfos[i].setNumBytes(bytesCount);
					replicaInfos[i].setBytesAcked(bytesCount);
					lostBloks[i].setNumBytes(bytesCount);
					data.finalizeBlock(lostBloks[i]);
					myMetrics.blocksWritten.inc();
					closeBlock(lostBloks[i], DataNode.EMPTY_DEL_HINT);

				}
				catch (IOException e)
				{
					LOG.warn("finalize and close i=" + i + " EXCEPTION " + e.toString() + " ------dsLog ");
					return;
				}
			}

		}
	}

	// seq RCR_DN_COMPUTE.1 5
	/***
	 * this method is to do the liner computing on the computeBloks with failednodevector and the result will be store
	 * as a new block named resultBlock. created at 2014-4-14.// modified at 2014-4-21,2014-4-24
	 * 
	 * @author ds
	 * @param computeBlocks
	 *            blocks take part in the liner computing
	 * @param failednodeVector
	 *            row vector in V matrix
	 * @throws IOException
	 */
	private LocatedBlock linearComputing(DatanodeInfo helperDatanodeInfo, Block[] computeBlocks, byte[] failednodeVector)
			throws IOException
	{
		// a : how many blocks a data node stores
		final int a = computeBlocks.length;

		final int unitSize = 512;

		// create inputStreams from computeBlocks

		InputStream[] ins = new InputStream[a];
		for (int i = 0; i < a; i++)
		{
			try
			{
				ins[i] = data.getBlockInputStream(computeBlocks[i]);
			}
			catch (IOException e)
			{
				LOG.warn("create inputStreams from computeBlocks" + " EXCEPTION " + e.toString() + " ------dsLog ");
				throw new IOException("could not create inputStreams from computeBlocks", e);
			}
		}

		// fake a block to store the result of computing
		Block resultBlock;
		long blockId = 12345;
		Random random = new Random(System.currentTimeMillis());
		long randomLong = random.nextLong();
		while (data.getReplica(randomLong) != null)
		{
			randomLong = random.nextLong();
		}
		blockId = randomLong;

		long numBytes = computeBlocks[0].getNumBytes();
		long generationStamp = computeBlocks[0].getGenerationStamp();
		resultBlock = new Block(blockId, numBytes, generationStamp);

		// create a tmp replicaInfo for result block
		ReplicaInPipelineInterface replicaInfo = null;
		try
		{
			replicaInfo = data.createTemporary(resultBlock);
		}
		catch (IOException e)
		{
			LOG.info("create tmp replicaInfo for result block" + " EXCEPTION " + e.toString() + " ------dsLog ");
			throw new IOException("could not create tmp replicaInfo for result block", e);

		}

		// create outs to block file and its checksum data file
		OutputStream out = null;
		DataOutputStream checksumOut = null;
		try
		{
			FSDataset.BlockWriteStreams streams = null;
			// need to reconsider
			streams = replicaInfo.createStreams(true, unitSize, 4);
			out = streams.dataOut;
			checksumOut = new DataOutputStream(new BufferedOutputStream(streams.checksumOut, SMALL_BUFFER_SIZE));
		}
		catch (IOException e)
		{
			LOG.info("create outs " + " EXCEPTION " + e.toString() + " ------dsLog ");
			throw new IOException("could not create outs for result block", e);
		}

		// create DataChescksum
		DataChecksum checksum = null;
		checksum = DataChecksum.newDataChecksum(DataChecksum.CHECKSUM_CRC32, unitSize);

		// write header to checksumOut

		try
		{
			BlockMetadataHeader.writeHeader(checksumOut, checksum);
		}
		catch (IOException e)
		{
			LOG.info("write header to checksumOut" + " EXCEPTION " + e.toString() + " ------dsLog ");
			throw new IOException("could not write header to checksumOut", e);
		}

		long bytesCount = 0;
		while (true)
		{
			int maxLen = -1;
			// read bytes to inBufs from ins
			byte inBufs[][] = new byte[a][unitSize];
			for (int i = 0; i < a; i++)
			{
				try
				{
					int inLen = -1;
					inLen = ins[i].read(inBufs[i]);
					if (inLen > maxLen)
					{
						maxLen = inLen;
					}
				}
				catch (IOException e)
				{
					LOG.info("read bytes to inBufs from ins" + " EXCEPTION " + e.toString() + " ------dsLog ");
					throw new IOException("could not read bytes to inBufs from ins", e);
				}
			}

			// judge maxLen
			if (maxLen == -1)
			{
				break;
			}
			LOG.info("maxLen = " + maxLen + " ------dsLog ");

			// liner computing, b1*v1+b2*v2+b3*v3+b4*v4+... write result to
			// outBuf
			byte outBuf[] = new byte[unitSize];
			for (int j = 0; j < maxLen; j++)
			{
				for (int i = 0; i < a; i++)
				{
					outBuf[j] ^= RSCoderProtocol.getRSP().mult(inBufs[i][j], failednodeVector[i]);
				}
			}
			// write outBuf to out

			try
			{
				out.write(outBuf, 0, maxLen);
			}
			catch (IOException e)
			{
				LOG.info("write outBuf to out" + " EXCEPTION " + e.toString() + " ------dsLog ");
				throw new IOException("could not write outBuf to out", e);
			}

			// compute checksum
			checksum.update(outBuf, 0, maxLen);
			int integer = (int) checksum.getValue();
			byte[] checksumValueBuf = new byte[4];
			checksumValueBuf[0] = (byte) ((integer >>> 24) & 0xFF);
			checksumValueBuf[1] = (byte) ((integer >>> 16) & 0xFF);
			checksumValueBuf[2] = (byte) ((integer >>> 8) & 0xFF);
			checksumValueBuf[3] = (byte) ((integer >>> 0) & 0xFF);
			checksum.reset();
			// write result to checksumOut

			try
			{
				checksumOut.write(checksumValueBuf);
			}
			catch (IOException e)
			{
				LOG.info("write result to checksumOut" + " EXCEPTION " + e.toString() + " ------dsLog ");
				throw new IOException("could not write result to checksumOut", e);
			}

			// bytesCount update
			bytesCount += maxLen;

		}
		// flush and close outs
		try
		{
			out.flush();
			out.close();

			checksumOut.flush();
			checksumOut.close();
		}
		catch (IOException e)
		{
			LOG.info("flush and close outs" + " EXCEPTION " + e.toString() + " ------dsLog ");
			throw new IOException("could not flush and close outs", e);
		}
		// finalize replica partly
		LOG.info("bytesCount: " + bytesCount + " ------dsLog ");

		try
		{
			resultBlock.setNumBytes(bytesCount);
			replicaInfo.setNumBytes(bytesCount);
			replicaInfo.setBytesAcked(bytesCount);
			// 4-29
			replicaInfo.setLastChecksumAndDataLen(bytesCount, null);
			// data.finalizeBlock(resultBlock); // modified at 2014-4-21
			// myMetrics.blocksWritten.inc();
			// closeBlock(resultBlock, DataNode.EMPTY_DEL_HINT);// modified at
			// 2014-4-21
		}
		// catch (IOException e)
		catch (Exception e) // modified at 2014-4-21
		{
			LOG.info("finalized partly" + " EXCEPTION " + e.toString() + " ------dsLog ");
			throw new IOException("could not finalized partly", e);
		}
		// construct LocatedBlock with result block
		LocatedBlock resultLocatedBlock = new LocatedBlock(resultBlock, new DatanodeInfo[]
		{ helperDatanodeInfo });
		return resultLocatedBlock;
	}
  
  /**
   * After a block becomes finalized, a datanode increases metric counter,
   * notifies namenode, and adds it to the block scanner
   * @param block
   * @param delHint
   */
  void closeBlock(Block block, String delHint) {
    myMetrics.blocksWritten.inc();
    notifyNamenodeReceivedBlock(block, delHint);
    if (blockScanner != null) {
      blockScanner.addBlock(block);
    }
  }

  //removed by xianyu
//  /**
//   * Tongxin 
//   * this function is used to collect the status of the datanode,and store them into
//   * dnStat.   
//   */
//   public void collectDatanodeStat() 
//   {
//	   try{
//	    dnStat.calCpuUsed();
//	    dnStat.calMemUsed();
//	    dnStat.calIoUsed();
//	    
//	     }
//	   catch(Exception e)
//	      {
//	    	  e.printStackTrace();
//	      }
//   }

  /**
   * No matter what kind of exception we get, keep retrying to offerService().
   * That's the loop that connects to the NameNode and provides basic DataNode
   * functionality.
   *
   * Only stop when "shouldRun" is turned off (which can only happen at shutdown).
   */
  public void run() {
    LOG.info(dnRegistration + "In DataNode.run, data = " + data);

    // start dataXceiveServer
    dataXceiverServer.start();
    ipcServer.start();
    dnStator.activate();//add by xianyu. to activate the Datanode Stator.

    while (shouldRun) {
      try {
        startDistributedUpgradeIfNeeded();
        offerService();
      } catch (Exception ex) {
        LOG.error("Exception: " + StringUtils.stringifyException(ex));
        if (shouldRun) {
          try {
            Thread.sleep(5000);
          } catch (InterruptedException ie) {
          }
        }
      }
    }
        
    LOG.info(dnRegistration + ":Finishing DataNode in: "+data);
    shutdown();
  }
    
  /** Start a single datanode daemon and wait for it to finish.
   *  If this thread is specifically interrupted, it will stop waiting.
   */
  public static void runDatanodeDaemon(DataNode dn) throws IOException {
    if (dn != null) {
      //register datanode
      dn.register();
      dn.dataNodeThread = new Thread(dn, dnThreadName);
      dn.dataNodeThread.setDaemon(true); // needed for JUnit testing
      dn.dataNodeThread.start();
    }
  }
  
  static boolean isDatanodeUp(DataNode dn) {
    return dn.dataNodeThread != null && dn.dataNodeThread.isAlive();
  }

  /** Instantiate a single datanode object. This must be run by invoking
   *  {@link DataNode#runDatanodeDaemon(DataNode)} subsequently. 
   */
  public static DataNode instantiateDataNode(String args[],
                                      Configuration conf) throws IOException {
    return instantiateDataNode(args, conf, null);
  }
  
  /** Instantiate a single datanode object, along with its secure resources. 
   * This must be run by invoking{@link DataNode#runDatanodeDaemon(DataNode)} 
   * subsequently. 
   */
  public static DataNode instantiateDataNode(String args [], Configuration conf,
      SecureResources resources) throws IOException {
    if (conf == null)
      conf = new HdfsConfiguration();
    
    if (args != null) {
      // parse generic hadoop options
      GenericOptionsParser hParser = new GenericOptionsParser(conf, args);
      args = hParser.getRemainingArgs();
    }
    
    if (!parseArguments(args, conf)) {
      printUsage();
      return null;
    }
    if (conf.get("dfs.network.script") != null) {
      LOG.error("This configuration for rack identification is not supported" +
          " anymore. RackID resolution is handled by the NameNode.");
      System.exit(-1);
    }
    Collection<URI> dataDirs = getStorageDirs(conf);
    dnThreadName = "DataNode: [" +
                    StringUtils.uriToString(dataDirs.toArray(new URI[0])) + "]";
   UserGroupInformation.setConfiguration(conf);
    SecurityUtil.login(conf, DFSConfigKeys.DFS_DATANODE_KEYTAB_FILE_KEY,
        DFSConfigKeys.DFS_DATANODE_USER_NAME_KEY);
    return makeInstance(dataDirs, conf, resources);
  }

  static Collection<URI> getStorageDirs(Configuration conf) {
    Collection<String> dirNames =
      conf.getTrimmedStringCollection(DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY);
    return Util.stringCollectionAsURIs(dirNames);
  }

  /** Instantiate & Start a single datanode daemon and wait for it to finish.
   *  If this thread is specifically interrupted, it will stop waiting.
   */
  public static DataNode createDataNode(String args[],
                                 Configuration conf) throws IOException {
    return createDataNode(args, conf, null);
  }
  
  /** Instantiate & Start a single datanode daemon and wait for it to finish.
   *  If this thread is specifically interrupted, it will stop waiting.
   */
  @InterfaceAudience.Private
  public static DataNode createDataNode(String args[], Configuration conf,
      SecureResources resources) throws IOException {
    DataNode dn = instantiateDataNode(args, conf, resources);
    runDatanodeDaemon(dn);
    return dn;
  }

  void join() {
    if (dataNodeThread != null) {
      try {
        dataNodeThread.join();
      } catch (InterruptedException e) {}
    }
  }

  /**
   * Make an instance of DataNode after ensuring that at least one of the
   * given data directories (and their parent directories, if necessary)
   * can be created.
   * @param dataDirs List of directories, where the new DataNode instance should
   * keep its files.
   * @param conf Configuration instance to use.
   * @param resources Secure resources needed to run under Kerberos
   * @return DataNode instance for given list of data dirs and conf, or null if
   * no directory from this directory list can be created.
   * @throws IOException
   */
  static DataNode makeInstance(Collection<URI> dataDirs, Configuration conf,
      SecureResources resources) throws IOException {
    LocalFileSystem localFS = FileSystem.getLocal(conf);
    FsPermission permission = new FsPermission(
        conf.get(DFSConfigKeys.DFS_DATANODE_DATA_DIR_PERMISSION_KEY,
                 DFSConfigKeys.DFS_DATANODE_DATA_DIR_PERMISSION_DEFAULT));
    ArrayList<File> dirs = getDataDirsFromURIs(dataDirs, localFS, permission);

    assert dirs.size() > 0 : "number of data directories should be > 0";
    return new DataNode(conf, dirs, resources);
  }

  // DataNode ctor expects AbstractList instead of List or Collection...
  static ArrayList<File> getDataDirsFromURIs(Collection<URI> dataDirs,
      LocalFileSystem localFS, FsPermission permission) throws IOException {
    ArrayList<File> dirs = new ArrayList<File>();
    StringBuilder invalidDirs = new StringBuilder();
    for (URI dirURI : dataDirs) {
      if (!"file".equalsIgnoreCase(dirURI.getScheme())) {
        LOG.warn("Unsupported URI schema in " + dirURI + ". Ignoring ...");
        invalidDirs.append("\"").append(dirURI).append("\" ");
        continue;
      }
      // drop any (illegal) authority in the URI for backwards compatibility
      File data = new File(dirURI.getPath());
      try {
        DiskChecker.checkDir(localFS, new Path(data.toURI()), permission);
        dirs.add(data);
      } catch (IOException e) {
        LOG.warn("Invalid directory in: "
                 + DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY + ": ", e);
        invalidDirs.append("\"").append(data.getCanonicalPath()).append("\" ");
      }
    }
    if (dirs.size() == 0)
      throw new IOException("All directories in "
          + DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY + " are invalid: "
          + invalidDirs);
    return dirs;
  }

  @Override
  public String toString() {
    return "DataNode{" +
      "data=" + data +
      (dnRegistration != null ?
          (", localName='" + dnRegistration.getName() + "'" +
              ", storageID='" + dnRegistration.getStorageID() + "'")
          : "") +
      ", xmitsInProgress=" + xmitsInProgress.get() +
      "}";
  }
  
  private static void printUsage() {
    System.err.println("Usage: java DataNode");
    System.err.println("           [-rollback]");
  }

  /**
   * Parse and verify command line arguments and set configuration parameters.
   *
   * @return false if passed argements are incorrect
   */
  private static boolean parseArguments(String args[], 
                                        Configuration conf) {
    int argsLen = (args == null) ? 0 : args.length;
    StartupOption startOpt = StartupOption.REGULAR;
    for(int i=0; i < argsLen; i++) {
      String cmd = args[i];
      if ("-r".equalsIgnoreCase(cmd) || "--rack".equalsIgnoreCase(cmd)) {
        LOG.error("-r, --rack arguments are not supported anymore. RackID " +
            "resolution is handled by the NameNode.");
        System.exit(-1);
      } else if ("-rollback".equalsIgnoreCase(cmd)) {
        startOpt = StartupOption.ROLLBACK;
      } else if ("-regular".equalsIgnoreCase(cmd)) {
        startOpt = StartupOption.REGULAR;
      } else
        return false;
    }
    setStartupOption(conf, startOpt);
    return true;
  }

  private static void setStartupOption(Configuration conf, StartupOption opt) {
    conf.set("dfs.datanode.startup", opt.toString());
  }

  static StartupOption getStartupOption(Configuration conf) {
    return StartupOption.valueOf(conf.get("dfs.datanode.startup",
                                          StartupOption.REGULAR.toString()));
  }

  /**
   * This methods  arranges for the data node to send the block report at the next heartbeat.
   */
  public void scheduleBlockReport(long delay) {
    if (delay > 0) { // send BR after random delay
      lastBlockReport = System.currentTimeMillis()
                            - ( blockReportInterval - R.nextInt((int)(delay)));
    } else { // send at next heartbeat
      lastBlockReport = lastHeartbeat - blockReportInterval;
    }
    resetBlockReportTime = true; // reset future BRs for randomness
  }
  
  
  /**
   * This method is used for testing. 
   * Examples are adding and deleting blocks directly.
   * The most common usage will be when the data node's storage is similated.
   * 
   * @return the fsdataset that stores the blocks
   */
  public FSDatasetInterface getFSDataset() {
    return data;
  }


  public static void secureMain(String args[], SecureResources resources) {
    try {
      StringUtils.startupShutdownMessage(DataNode.class, args, LOG);
      DataNode datanode = createDataNode(args, null, resources);
      if (datanode != null)
        datanode.join();
    } catch (Throwable e) {
      LOG.error(StringUtils.stringifyException(e));
      System.exit(-1);
    }
  }
  
  public static void main(String args[]) {
    secureMain(args, null);
  }

  public Daemon recoverBlocks(final Collection<RecoveringBlock> blocks) {
    Daemon d = new Daemon(threadGroup, new Runnable() {
      /** Recover a list of blocks. It is run by the primary datanode. */
      public void run() {
        for(RecoveringBlock b : blocks) {
          try {
            logRecoverBlock("NameNode", b.getBlock(), b.getLocations());
            recoverBlock(b);
          } catch (IOException e) {
            LOG.warn("recoverBlocks FAILED: " + b, e);
          }
        }
      }
    });
    d.start();
    return d;
  }

  // InterDataNodeProtocol implementation
  @Override // InterDatanodeProtocol
  public ReplicaRecoveryInfo initReplicaRecovery(RecoveringBlock rBlock)
  throws IOException {
    return data.initReplicaRecovery(rBlock);
  }

  /**
   * Convenience method, which unwraps RemoteException.
   * @throws IOException not a RemoteException.
   */
  private static ReplicaRecoveryInfo callInitReplicaRecovery(
      InterDatanodeProtocol datanode,
      RecoveringBlock rBlock) throws IOException {
    try {
      return datanode.initReplicaRecovery(rBlock);
    } catch(RemoteException re) {
      throw re.unwrapRemoteException();
    }
  }

  /**
   * Update replica with the new generation stamp and length.  
   */
  @Override // InterDatanodeProtocol
  public Block updateReplicaUnderRecovery(Block oldBlock,
                                          long recoveryId,
                                          long newLength) throws IOException {
    ReplicaInfo r =
      data.updateReplicaUnderRecovery(oldBlock, recoveryId, newLength);
    return new Block(r);
  }
//seq RCR_DN_BEGINH.2 3
	/**
	 * new comer inform a helper node to start rc in RC(regenerating code)recovery work. created at 2041-4-17. modified
	 * at 2014-4-24
	 * 
	 * @author ds
	 * @param computeBlocks
	 *            blocks who take part in liner computing
	 * @param failednodeVector
	 *            row failed node in vondemendMatrix
	 * @return the liner computing result
	 * @throws IOException
	 */
	// 2041-4-17
	// Block startRCRecovery(Block[] computeBlocks, byte[] failednodeVector);
	@Override
	public LocatedBlock beginRCRecovery(DatanodeInfo helperDatanodeInfo, Block[] computeBlocks, byte[] failednodeVector)
			throws IOException
	{
		LOG.info("beginRCRecovery()" + " ------dsLog ");
		return linearComputing(helperDatanodeInfo, computeBlocks, failednodeVector);
	}

	// seq RCR_DN_ENDH.2 4
	/**
	 * new comer inform a helper node to end rc in RC(regenerating code)recovery work. created at 2041-4-24
	 * 
	 * @author ds
	 */
	@Override
	public void endRCRecovery(Block block) throws IOException
	{
		LOG.info("endRCRecovery()" + " ------dsLog ");
		try
		{
			data.unfinalizeBlock(block);
		}
		catch (IOException e)
		{
			LOG.warn("delelte tmp block" + " EXCEPTION " + e.toString() + " ------dsLog ");
			throw new IOException("could not delelte tmp block", e);
		}
	}

  /** {@inheritDoc} */
  public long getProtocolVersion(String protocol, long clientVersion
      ) throws IOException {
    if (protocol.equals(InterDatanodeProtocol.class.getName())) {
      return InterDatanodeProtocol.versionID; 
    } else if (protocol.equals(ClientDatanodeProtocol.class.getName())) {
      return ClientDatanodeProtocol.versionID; 
    }
    throw new IOException("Unknown protocol to " + getClass().getSimpleName()
        + ": " + protocol);
  }

  /** A convenient class used in block recovery */
  static class BlockRecord { 
    final DatanodeID id;
    final InterDatanodeProtocol datanode;
    final ReplicaRecoveryInfo rInfo;
    
    BlockRecord(DatanodeID id,
                InterDatanodeProtocol datanode,
                ReplicaRecoveryInfo rInfo) {
      this.id = id;
      this.datanode = datanode;
      this.rInfo = rInfo;
    }

    /** {@inheritDoc} */
    public String toString() {
      return "block:" + rInfo + " node:" + id;
    }
  }

  /** Recover a block */
  private void recoverBlock(RecoveringBlock rBlock) throws IOException {
    Block block = rBlock.getBlock();
    DatanodeInfo[] targets = rBlock.getLocations();
    DatanodeID[] datanodeids = (DatanodeID[])targets;
    List<BlockRecord> syncList = new ArrayList<BlockRecord>(datanodeids.length);
    int errorCount = 0;

    //check generation stamps
    for(DatanodeID id : datanodeids) {
      try {
        InterDatanodeProtocol datanode = dnRegistration.equals(id)?
            this: DataNode.createInterDataNodeProtocolProxy(id, getConf(),
                socketTimeout);
        ReplicaRecoveryInfo info = callInitReplicaRecovery(datanode, rBlock);
        if (info != null &&
            info.getGenerationStamp() >= block.getGenerationStamp() &&
            info.getNumBytes() > 0) {
          syncList.add(new BlockRecord(id, datanode, info));
        }
      } catch (RecoveryInProgressException ripE) {
        InterDatanodeProtocol.LOG.warn(
            "Recovery for replica " + block + " on data-node " + id
            + " is already in progress. Recovery id = "
            + rBlock.getNewGenerationStamp() + " is aborted.", ripE);
        return;
      } catch (IOException e) {
        ++errorCount;
        InterDatanodeProtocol.LOG.warn(
            "Failed to obtain replica info for block (=" + block 
            + ") from datanode (=" + id + ")", e);
      }
    }

    if (errorCount == datanodeids.length) {
      throw new IOException("All datanodes failed: block=" + block
          + ", datanodeids=" + Arrays.asList(datanodeids));
    }

    syncBlock(rBlock, syncList);
  }

  /** Block synchronization */
  void syncBlock(RecoveringBlock rBlock,
                         List<BlockRecord> syncList) throws IOException {
    Block block = rBlock.getBlock();
    long recoveryId = rBlock.getNewGenerationStamp();
    if (LOG.isDebugEnabled()) {
      LOG.debug("block=" + block + ", (length=" + block.getNumBytes()
          + "), syncList=" + syncList);
    }

    // syncList.isEmpty() means that all data-nodes do not have the block
    // or their replicas have 0 length.
    // The block can be deleted.
    if (syncList.isEmpty()) {
      namenode.commitBlockSynchronization(block, recoveryId, 0,
          true, true, DatanodeID.EMPTY_ARRAY);
      return;
    }

    // Calculate the best available replica state.
    ReplicaState bestState = ReplicaState.RWR;
    long finalizedLength = -1;
    for(BlockRecord r : syncList) {
      assert r.rInfo.getNumBytes() > 0 : "zero length replica";
      ReplicaState rState = r.rInfo.getOriginalReplicaState(); 
      if(rState.getValue() < bestState.getValue())
        bestState = rState;
      if(rState == ReplicaState.FINALIZED) {
        if(finalizedLength > 0 && finalizedLength != r.rInfo.getNumBytes())
          throw new IOException("Inconsistent size of finalized replicas. " +
              "Replica " + r.rInfo + " expected size: " + finalizedLength);
        finalizedLength = r.rInfo.getNumBytes();
      }
    }

    // Calculate list of nodes that will participate in the recovery
    // and the new block size
    List<BlockRecord> participatingList = new ArrayList<BlockRecord>();
    Block newBlock = new Block(block.getBlockId(), -1, recoveryId);
    switch(bestState) {
    case FINALIZED:
      assert finalizedLength > 0 : "finalizedLength is not positive";
      for(BlockRecord r : syncList) {
        ReplicaState rState = r.rInfo.getOriginalReplicaState();
        if(rState == ReplicaState.FINALIZED ||
           rState == ReplicaState.RBW &&
                      r.rInfo.getNumBytes() == finalizedLength)
          participatingList.add(r);
      }
      newBlock.setNumBytes(finalizedLength);
      break;
    case RBW:
    case RWR:
      long minLength = Long.MAX_VALUE;
      for(BlockRecord r : syncList) {
        ReplicaState rState = r.rInfo.getOriginalReplicaState();
        if(rState == bestState) {
          minLength = Math.min(minLength, r.rInfo.getNumBytes());
          participatingList.add(r);
        }
      }
      newBlock.setNumBytes(minLength);
      break;
    case RUR:
    case TEMPORARY:
      assert false : "bad replica state: " + bestState;
    }

    List<DatanodeID> failedList = new ArrayList<DatanodeID>();
    List<DatanodeID> successList = new ArrayList<DatanodeID>();
    for(BlockRecord r : participatingList) {
      try {
        Block reply = r.datanode.updateReplicaUnderRecovery(
            r.rInfo, recoveryId, newBlock.getNumBytes());
        assert reply.equals(newBlock) &&
               reply.getNumBytes() == newBlock.getNumBytes() :
          "Updated replica must be the same as the new block.";
        successList.add(r.id);
      } catch (IOException e) {
        InterDatanodeProtocol.LOG.warn("Failed to updateBlock (newblock="
            + newBlock + ", datanode=" + r.id + ")", e);
        failedList.add(r.id);
      }
    }

    // If any of the data-nodes failed, the recovery fails, because
    // we never know the actual state of the replica on failed data-nodes.
    // The recovery should be started over.
    if(!failedList.isEmpty()) {
      StringBuilder b = new StringBuilder();
      for(DatanodeID id : failedList) {
        b.append("\n  " + id);
      }
      throw new IOException("Cannot recover " + block + ", the following "
          + failedList.size() + " data-nodes failed {" + b + "\n}");
    }

    // Notify the name-node about successfully recovered replicas.
    DatanodeID[] nlist = successList.toArray(new DatanodeID[successList.size()]);
    namenode.commitBlockSynchronization(block,
        newBlock.getGenerationStamp(), newBlock.getNumBytes(), true, false,
        nlist);
  }
  
  private static void logRecoverBlock(String who,
      Block block, DatanodeID[] targets) {
    StringBuilder msg = new StringBuilder(targets[0].getName());
    for (int i = 1; i < targets.length; i++) {
      msg.append(", " + targets[i].getName());
    }
    LOG.info(who + " calls recoverBlock(block=" + block
        + ", targets=[" + msg + "])");
  }

  // ClientDataNodeProtocol implementation
  /** {@inheritDoc} */
  @Override // ClientDataNodeProtocol
  public long getReplicaVisibleLength(final Block block) throws IOException {
    if (isBlockTokenEnabled) {
      Set<TokenIdentifier> tokenIds = UserGroupInformation.getCurrentUser()
          .getTokenIdentifiers();
      if (tokenIds.size() != 1) {
        throw new IOException("Can't continue with getReplicaVisibleLength() "
            + "authorization since none or more than one BlockTokenIdentifier "
            + "is found.");
      }
      for (TokenIdentifier tokenId : tokenIds) {
        BlockTokenIdentifier id = (BlockTokenIdentifier) tokenId;
        if (LOG.isDebugEnabled()) {
          LOG.debug("Got: " + id.toString());
        }
        blockTokenSecretManager.checkAccess(id, null, block,
            BlockTokenSecretManager.AccessMode.WRITE);
      }
    }

    return data.getReplicaVisibleLength(block);
  }
  
  // Determine a Datanode's streaming address
  public static InetSocketAddress getStreamingAddr(Configuration conf) {
    return NetUtils.createSocketAddr(
        conf.get("dfs.datanode.address", "0.0.0.0:50010"));
  }
  @Override // DataNodeMXBean
  public String getVersion() {
    return VersionInfo.getVersion();
  }
  
  @Override // DataNodeMXBean
  public String getRpcPort(){
    InetSocketAddress ipcAddr = NetUtils.createSocketAddr(
        this.getConf().get("dfs.datanode.ipc.address"));
    return Integer.toString(ipcAddr.getPort());
  }

  @Override // DataNodeMXBean
  public String getHttpPort(){
    return this.getConf().get("dfs.datanode.info.port");
  }

  public int getInfoPort() {
    return this.infoServer.getPort();
  }

  @Override // DataNodeMXBean
  public String getNamenodeAddress(){
    return nameNodeAddr.getHostName();
  }

  /**
   * Returned information is a JSON representation of a map with 
   * volume name as the key and value is a map of volume attribute 
   * keys to its values
   */
  @Override // DataNodeMXBean
  public String getVolumeInfo() {
    final Map<String, Object> info = new HashMap<String, Object>();
    Collection<VolumeInfo> volumes = ((FSDataset)this.data).getVolumeInfo();
    for (VolumeInfo v : volumes) {
      final Map<String, Object> innerInfo = new HashMap<String, Object>();
      innerInfo.put("usedSpace", v.usedSpace);
      innerInfo.put("freeSpace", v.freeSpace);
      innerInfo.put("reservedSpace", v.reservedSpace);
      info.put(v.directory, innerInfo);
    }
    return JSON.toString(info);
  }
  
  class CumulusRecover implements Runnable{
	  CodingMatrix matrix;
	  

	@Override
	public void run() {
		// TODO Auto-generated method stub
		
	}
	  
  }
}
