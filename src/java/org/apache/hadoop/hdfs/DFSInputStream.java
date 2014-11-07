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
package org.apache.hadoop.hdfs;



import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;


import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.RSCoderProtocol;
import org.apache.hadoop.hdfs.protocol.RegeneratingCodeMatrix;
import org.apache.hadoop.hdfs.security.token.block.InvalidBlockTokenException;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.protocol.CodingMatrix;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.StringUtils;





/****************************************************************
 * DFSInputStream provides bytes from a named file.  It handles 
 * negotiation of the namenode and various datanodes as necessary.
 ****************************************************************/
@InterfaceAudience.Private
public class DFSInputStream extends FSInputStream {
  private final SocketCache socketCache;

  private final DFSClient dfsClient;
  private boolean closed = false;

  private final String src;
  private long prefetchSize;
  private BlockReader blockReader = null;
  private boolean verifyChecksum;
  private LocatedBlocks locatedBlocks = null;
 // private long lastBlockBeingWrittenLength = 0;
  private DatanodeInfo currentNode = null;
  private Block currentBlock = null;
  private long pos = 0;
  private long blockEnd = -1;
  private int blockIndex = 0; //
  
  private Vector<DataPool> dps;
  private CodingMatrix matrix;
  //seq 12.1 2
  // modified by ds at 2014-5-9
  // modified modified by ds begins
  // if not rcr, k=k_nodes=k_blocks
  // //private int k ;
	private int k_blocks;
	private int k_nodes;
	private int B;
	// modified by ds ends
  private int n;
  private int packetSize = 65024;
  private long fileLength = 0;
  private int[] usingDN;
  private byte[] Bbyte ;
  private int BbyteLength = 0;
  private int BbytePosition = 0;
  private long startTime = 0;
  private RSCoderProtocol rsp= RSCoderProtocol.getRSP();
   
  /**
   * This variable tracks the number of failures since the start of the
   * most recent user-facing operation. That is to say, it should be reset
   * whenever the user makes a call on this stream, and if at any point
   * during the retry logic, the failure count exceeds a threshold,
   * the errors will be thrown back to the operation.
   *
   * Specifically this counts the number of times the client has gone
   * back to the namenode to get a new list of block locations, and is
   * capped at maxBlockAcquireFailures
   */
  private int failures = 0;
  private int timeWindow = 3000; // wait time window (in msec) if BlockMissingException is caught

  /* XXX Use of CocurrentHashMap is temp fix. Need to fix 
   * parallel accesses to DFSInputStream (through ptreads) properly */
  private ConcurrentHashMap<DatanodeInfo, DatanodeInfo> deadNodes = 
             new ConcurrentHashMap<DatanodeInfo, DatanodeInfo>();
  private int buffersize = 1;
  
  private byte[] oneByteBuf = new byte[1]; // used for 'int read()'

  private int nCachedConnRetry;

  void addToDeadNodes(DatanodeInfo dnInfo) {
    deadNodes.put(dnInfo, dnInfo);
  }
  
  DFSInputStream(DFSClient dfsClient, String src, int buffersize, boolean verifyChecksum
                 ) throws IOException, UnresolvedLinkException {
    this.dfsClient = dfsClient;
    this.verifyChecksum = verifyChecksum;
    this.buffersize = buffersize;
    this.src = src;
    this.socketCache = dfsClient.socketCache;
    prefetchSize = this.dfsClient.conf.getLong(DFSConfigKeys.DFS_CLIENT_READ_PREFETCH_SIZE_KEY,
        10 * dfsClient.defaultBlockSize);
    timeWindow = this.dfsClient.conf.getInt(
        DFSConfigKeys.DFS_CLIENT_RETRY_WINDOW_BASE, timeWindow);
    nCachedConnRetry = this.dfsClient.conf.getInt(
        DFSConfigKeys.DFS_CLIENT_CACHED_CONN_RETRY_KEY,
        DFSConfigKeys.DFS_CLIENT_CACHED_CONN_RETRY_DEFAULT);
    openInfo();
  }

  /**
   * Grab the open-file info from namenode
   * revised by czl @ 2013/3/11
   */
  synchronized void openInfo() throws IOException, UnresolvedLinkException {
   // LocatedBlocks newInfo = DFSClient.callGetBlockLocations(dfsClient.namenode, src, 0, prefetchSize);
	LocatedBlocks newInfo = dfsClient.namenode.getAllBlocksLocations(src);
    if (DFSClient.LOG.isDebugEnabled()) {
      DFSClient.LOG.debug("newInfo = " + newInfo);
    }
    if (newInfo == null) {
      throw new IOException("Cannot open filename " + src);
    }

    if (locatedBlocks != null) {
      Iterator<LocatedBlock> oldIter = locatedBlocks.getLocatedBlocks().iterator();
      Iterator<LocatedBlock> newIter = newInfo.getLocatedBlocks().iterator();
      while (oldIter.hasNext() && newIter.hasNext()) {
        if (! oldIter.next().getBlock().equals(newIter.next().getBlock())) {
          throw new IOException("Blocklist for " + src + " has changed!");
        }
      }
    }
    locatedBlocks = newInfo;
    /*
    lastBlockBeingWrittenLength = 0;
    if (!locatedBlocks.isLastBlockComplete()) {
      final LocatedBlock last = locatedBlocks.getLastLocatedBlock();
      if (last != null) {
        final long len = readBlockLength(last);
        last.getBlock().setNumBytes(len);
        lastBlockBeingWrittenLength = len; 
      }
    }

    currentNode = null;
    */
    
    matrix = dfsClient.namenode.getCodingMatrix(src);
    dps = new Vector<DataPool>();
 // seq 12.1 1
	// modified by ds at 2014-5-9
	// modified modified by ds begins
	// // k = matrix.getRow();
	boolean isRCRecovery = RegeneratingCodeMatrix.isRegeneratingCodeRecovery();
	if (isRCRecovery)
	{

		k_nodes = matrix.getK();
		k_blocks = k_nodes * matrix.getA();
		B = matrix.getB();
	}
	else
	{
		k_blocks = matrix.getRow();
		k_nodes = k_blocks * 1;
		B = k_blocks;
	}
	// modified by ds ends
    n = matrix.getColumn();
    //DFSClient.LOG.info("matrix: "+matrix.toString());
    usingDN = new int[k_blocks];
    
    int count = 0;
    
    while(count < k_blocks && blockIndex < n){
		LocatedBlock block = locatedBlocks.getLocatedBlocks().get(blockIndex);
	    DNAddrPair retval = chooseDataNode(block);
       //DatanodeInfo chosenNode = retval.info;
       InetSocketAddress targetAddr = retval.addr;
       BlockReader reader = null;
     
       //DFSClient.LOG.info("read block "+block.toString()+"........."+buffersize);
       try {
	          Token<BlockTokenIdentifier> blockToken = block.getBlockToken();
	         // DFSClient.LOG.info(block.getStartOffset()+" blocksize:"+block.getBlockSize());	         
	          reader = getBlockReader(targetAddr, src,
	                                  block.getBlock(),
	                                  blockToken,
	                                  0, block.getBlockSize(), buffersize,
	                                  verifyChecksum, dfsClient.clientName);
	          dps.add(new DataPool(reader,0, 2*1024*1024));	
	          // seq READ.1 4
				// modified by ds at 2014-5-9
				// modified modified by ds begins
				// //usingDN[count] = blockIndex;
				if (isRCRecovery)
				{
					usingDN[count] = blockIndex / matrix.getA();
				}
				else
				{
					usingDN[count] = blockIndex;

				}
				// modified by ds ends 	           
	          count++;
	          DFSClient.LOG.info("reading block from DataNode: " + retval.addr);
	          
       	}
       catch (Exception e) {
			// TODO: handle exception
       		//DFSClient.LOG.info(e.toString());
		}
       blockIndex++;
	  }
    if (count < k_blocks) {
		throw new IOException("file has failed.............czl");
	}
    //DFSClient.LOG.info("in openinfo count: "+count+" blockIndex: "+blockIndex);
    
    fileLength = getFileLength();
    //DFSClient.LOG.info("fileLength: "+fileLength);
    
    Bbyte = new byte[packetSize*B];
    startTime = System.currentTimeMillis();
  }

  
  /** Read the block length from one of the datanodes. 
 * @throws UnresolvedLinkException */
  /*
  private long readBlockLength(LocatedBlock locatedblock) throws IOException {
    if (locatedblock == null || locatedblock.getLocations().length == 0) {
      return 0;
    }
    int replicaNotFoundCount = locatedblock.getLocations().length;
    
    for(DatanodeInfo datanode : locatedblock.getLocations()) {
      ClientDatanodeProtocol cdp = null;
      
      try {
        cdp = DFSClient.createClientDatanodeProtocolProxy(
        datanode, dfsClient.conf, dfsClient.socketTimeout, locatedblock);
        
        final long n = cdp.getReplicaVisibleLength(locatedblock.getBlock());
        
        if (n >= 0) {
          return n;
        }
      }
      catch(IOException ioe) {
        if (ioe instanceof RemoteException &&
          (((RemoteException) ioe).unwrapRemoteException() instanceof
            ReplicaNotFoundException)) {
          // special case : replica might not be on the DN, treat as 0 length
          replicaNotFoundCount--;
        }
        
        if (DFSClient.LOG.isDebugEnabled()) {
          DFSClient.LOG.debug("Failed to getReplicaVisibleLength from datanode "
              + datanode + " for block " + locatedblock.getBlock(), ioe);
        }
      } finally {
        if (cdp != null) {
          RPC.stopProxy(cdp);
        }
      }
    }

    // Namenode told us about these locations, but none know about the replica
    // means that we hit the race between pipeline creation start and end.
    // we require all 3 because some other exception could have happened
    // on a DN that has it.  we want to report that error
    if (replicaNotFoundCount == 0) {
      return 0;
    }

    throw new IOException("Cannot obtain block length for " + locatedblock);
  }
  */
  public synchronized long getFileLength() throws UnresolvedLinkException {
//    return locatedBlocks == null? 0:
//        locatedBlocks.getFileLength() + lastBlockBeingWrittenLength;
	  return dfsClient.namenode.getFileLength(src);
  }
  /**
   * Returns the datanode from which the stream is currently reading.
   */
  public DatanodeInfo getCurrentDatanode() {
    return currentNode;
  }

  /**
   * Returns the block containing the target position. 
   */
  public Block getCurrentBlock() {
    return currentBlock;
  }

  /**
   * Return collection of blocks that has already been located.
   */
  synchronized List<LocatedBlock> getAllBlocks() throws IOException {
    return getBlockRange(0, getFileLength());
  }

  /**
   * Get block at the specified position.
   * Fetch it from the namenode if not cached.
   * 
   * @param offset
   * @param updatePosition whether to update current position
   * @return located block
   * @throws IOException
   */
  private synchronized LocatedBlock getBlockAt(long offset,
      boolean updatePosition) throws IOException {
    assert (locatedBlocks != null) : "locatedBlocks is null";

    final LocatedBlock blk;

    //check offset
    if (offset < 0 || offset >= getFileLength()) {
      throw new IOException("offset < 0 || offset > getFileLength(), offset="
          + offset
          + ", updatePosition=" + updatePosition
          + ", locatedBlocks=" + locatedBlocks);
    }
    else if (offset >= locatedBlocks.getFileLength()) {
      // offset to the portion of the last block,
      // which is not known to the name-node yet;
      // getting the last block 
      blk = locatedBlocks.getLastLocatedBlock();
    }
    else {
      // search cached blocks first
      int targetBlockIdx = locatedBlocks.findBlock(offset);
      if (targetBlockIdx < 0) { // block is not cached
        targetBlockIdx = LocatedBlocks.getInsertIndex(targetBlockIdx);
        // fetch more blocks
        LocatedBlocks newBlocks;
        newBlocks = DFSClient.callGetBlockLocations(dfsClient.namenode, src, offset, prefetchSize);
        assert (newBlocks != null) : "Could not find target position " + offset;
        locatedBlocks.insertRange(targetBlockIdx, newBlocks.getLocatedBlocks());
      }
      blk = locatedBlocks.get(targetBlockIdx);
    }

    // update current position
    if (updatePosition) {
      pos = offset;
      blockEnd = blk.getStartOffset() + blk.getBlockSize() - 1;
      currentBlock = blk.getBlock();
    }
    return blk;
  }

  /** Fetch a block from namenode and cache it */
  private synchronized void fetchBlockAt(long offset) throws IOException {
    int targetBlockIdx = locatedBlocks.findBlock(offset);
    if (targetBlockIdx < 0) { // block is not cached
      targetBlockIdx = LocatedBlocks.getInsertIndex(targetBlockIdx);
    }
    // fetch blocks
    LocatedBlocks newBlocks;
    newBlocks = DFSClient.callGetBlockLocations(dfsClient.namenode, src, offset, prefetchSize);
    if (newBlocks == null) {
      throw new IOException("Could not find target position " + offset);
    }
    locatedBlocks.insertRange(targetBlockIdx, newBlocks.getLocatedBlocks());
  }

  /**
   * Get blocks in the specified range.
   * Fetch them from the namenode if not cached.
   * 
   * @param offset
   * @param length
   * @return consequent segment of located blocks
   * @throws IOException
   */
  private synchronized List<LocatedBlock> getBlockRange(long offset, 
                                                        long length) 
                                                      throws IOException {
    final List<LocatedBlock> blocks;
    if (locatedBlocks.isLastBlockComplete()) {
      blocks = getFinalizedBlockRange(offset, length);
    }
    else {
      if (length + offset > locatedBlocks.getFileLength()) {
        length = locatedBlocks.getFileLength() - offset;
      }
      blocks = getFinalizedBlockRange(offset, length);
      blocks.add(locatedBlocks.getLastLocatedBlock());
    }
    return blocks;
  }

  /**
   * Get blocks in the specified range.
   * Includes only the complete blocks.
   * Fetch them from the namenode if not cached.
   */
  private synchronized List<LocatedBlock> getFinalizedBlockRange(
      long offset, long length) throws IOException {
    assert (locatedBlocks != null) : "locatedBlocks is null";
    List<LocatedBlock> blockRange = new ArrayList<LocatedBlock>();
    // search cached blocks first
    int blockIdx = locatedBlocks.findBlock(offset);
    if (blockIdx < 0) { // block is not cached
      blockIdx = LocatedBlocks.getInsertIndex(blockIdx);
    }
    long remaining = length;
    long curOff = offset;
    while(remaining > 0) {
      LocatedBlock blk = null;
      if(blockIdx < locatedBlocks.locatedBlockCount())
        blk = locatedBlocks.get(blockIdx);
      if (blk == null || curOff < blk.getStartOffset()) {
        LocatedBlocks newBlocks;
        newBlocks = DFSClient.callGetBlockLocations(dfsClient.namenode, src, curOff, remaining);
        locatedBlocks.insertRange(blockIdx, newBlocks.getLocatedBlocks());
        continue;
      }
      assert curOff >= blk.getStartOffset() : "Block not found";
      blockRange.add(blk);
      long bytesRead = blk.getStartOffset() + blk.getBlockSize() - curOff;
      remaining -= bytesRead;
      curOff += bytesRead;
      blockIdx++;
    }
    return blockRange;
  }

  /**
   * Open a DataInputStream to a DataNode so that it can be read from.
   * We get block ID and the IDs of the destinations at startup, from the namenode.
   */
  private synchronized DatanodeInfo blockSeekTo(long target) throws IOException {
    if (target >= getFileLength()) {
      throw new IOException("Attempted to read past end of file");
    }

    // Will be getting a new BlockReader.
    if (blockReader != null) {
      closeBlockReader(blockReader);
      blockReader = null;
    }

    //
    // Connect to best DataNode for desired Block, with potential offset
    //
    DatanodeInfo chosenNode = null;
    int refetchToken = 1; // only need to get a new access token once
    
    while (true) {
      //
      // Compute desired block
      //
      LocatedBlock targetBlock = getBlockAt(target, true);
      assert (target==pos) : "Wrong postion " + pos + " expect " + target;
      long offsetIntoBlock = target - targetBlock.getStartOffset();

      DNAddrPair retval = chooseDataNode(targetBlock);
      chosenNode = retval.info;
      InetSocketAddress targetAddr = retval.addr;

      try {
        Block blk = targetBlock.getBlock();
        Token<BlockTokenIdentifier> accessToken = targetBlock.getBlockToken();
        
        blockReader = getBlockReader(
            targetAddr, src, blk,
            accessToken,
            offsetIntoBlock, blk.getNumBytes() - offsetIntoBlock,
            buffersize, verifyChecksum, dfsClient.clientName);
        return chosenNode;
      } catch (IOException ex) {
        if (ex instanceof InvalidBlockTokenException && refetchToken > 0) {
          DFSClient.LOG.info("Will fetch a new access token and retry, " 
              + "access token was invalid when connecting to " + targetAddr
              + " : " + ex);
          /*
           * Get a new access token and retry. Retry is needed in 2 cases. 1)
           * When both NN and DN re-started while DFSClient holding a cached
           * access token. 2) In the case that NN fails to update its
           * access key at pre-set interval (by a wide margin) and
           * subsequently restarts. In this case, DN re-registers itself with
           * NN and receives a new access key, but DN will delete the old
           * access key from its memory since it's considered expired based on
           * the estimated expiration date.
           */
          refetchToken--;
          fetchBlockAt(target);
        } else {
          DFSClient.LOG.info("Failed to connect to " + targetAddr
              + ", add to deadNodes and continue", ex);
          // Put chosen node into dead list, continue
          addToDeadNodes(chosenNode);
        }
      }
    }
  }

  /**
   * Close it down!
   */
  @Override
  public synchronized void close() throws IOException {
    if (closed) {
      return;
    }
    dfsClient.checkOpen();

    if (blockReader != null) {
      closeBlockReader(blockReader);
      blockReader = null;
    }
    super.close();
    closed = true;
  }

  @Override
  public synchronized int read() throws IOException {
    int ret = read( oneByteBuf, 0, 1 );
    return ( ret <= 0 ) ? -1 : (oneByteBuf[0] & 0xff);
  }

  /* This is a used by regular read() and handles ChecksumExceptions.
   * name readBuffer() is chosen to imply similarity to readBuffer() in
   * ChecksumFileSystem
   */ 
  
//  private synchronized int readBuffer(byte buf[], int off, int len) 
//                                                  throws IOException {
//    IOException ioe;
//    
//    /* we retry current node only once. So this is set to true only here.
//     * Intention is to handle one common case of an error that is not a
//     * failure on datanode or client : when DataNode closes the connection
//     * since client is idle. If there are other cases of "non-errors" then
//     * then a datanode might be retried by setting this to true again.
//     */
//    boolean retryCurrentNode = true;
//
//    while (true) {
//      // retry as many times as seekToNewSource allows.
//      try {
//        return blockReader.read(buf, off, len);
//      } catch ( ChecksumException ce ) {
//        DFSClient.LOG.warn("Found Checksum error for " + currentBlock + " from " +
//                 currentNode.getName() + " at " + ce.getPos());          
//        dfsClient.reportChecksumFailure(src, currentBlock, currentNode);
//        ioe = ce;
//        retryCurrentNode = false;
//      } catch ( IOException e ) {
//        if (!retryCurrentNode) {
//          DFSClient.LOG.warn("Exception while reading from " + currentBlock +
//                   " of " + src + " from " + currentNode + ": " +
//                   StringUtils.stringifyException(e));
//        }
//        ioe = e;
//      }
//      boolean sourceFound = false;
//      if (retryCurrentNode) {
//        /* possibly retry the same node so that transient errors don't
//         * result in application level failures (e.g. Datanode could have
//         * closed the connection because the client is idle for too long).
//         */ 
//        sourceFound = seekToBlockSource(pos);
//      } else {
//        addToDeadNodes(currentNode);
//        sourceFound = seekToNewSource(pos);
//      }
//      if (!sourceFound) {
//        throw ioe;
//      }
//      retryCurrentNode = false;
//    }
//  }
  
  /**
   * Read the entire buffer.
   * revised by czl @2013/3/11
   */
  @Override
  public synchronized int read(byte buf[], int off, int len) throws IOException {
    dfsClient.checkOpen();
    /*
    if (closed) {
      throw new IOException("Stream closed");
    }
    failures = 0;
    if (pos < getFileLength()) {
      int retries = 2;
      while (retries > 0) {
        try {
          if (pos > blockEnd) {
            currentNode = blockSeekTo(pos);
          }
          int realLen = (int) Math.min((long) len, (blockEnd - pos + 1L));
          int result = readBuffer(buf, off, realLen);
          
          if (result >= 0) {
            pos += result;
          } else {
            // got a EOS from reader though we expect more data on it.
            throw new IOException("Unexpected EOS from the reader");
          }
          if (dfsClient.stats != null && result != -1) {
            dfsClient.stats.incrementBytesRead(result);
          }
          return result;
        } catch (ChecksumException ce) {
          throw ce;            
        } catch (IOException e) {
          if (retries == 1) {
            DFSClient.LOG.warn("DFS Read: " + StringUtils.stringifyException(e));
          }
          blockEnd = -1;
          if (currentNode != null) { addToDeadNodes(currentNode); }
          if (--retries == 0) {
            throw e;
          }
        }
      }
    }
    */
    
    /*
     * buf 每次Decoding
     */
    len = (buf.length-off>len)?len:(buf.length-off);

   // DFSClient.LOG.info("filelength: !!!!!!!"+fileLength);
    //DFSClient.LOG.info("BbyteLength: "+BbyteLength+" Bbyteposition: "+BbytePosition);
	if (BbyteLength > 0)
	{
	   	 len = (len>BbyteLength)?BbyteLength:len;
	  // 	DFSClient.LOG.info("len: "+len+" buffersize: "+buf.length);//128K
	   	 System.arraycopy(Bbyte, BbytePosition, buf, off, len);
	   	 BbytePosition += len;
	   	 BbyteLength -= len;
	   	 if (fileLength>=len)
	   	 {
			 fileLength -= len;
			 //DFSClient.LOG.info("return 1 len: "+len);
			 return len;
	   	 }
		 else 
		 {
			 //DFSClient.LOG.info("return 1 len: "+fileLength);
			 DFSClient.LOG.info("Cumulus read time consumption :"+ (System.currentTimeMillis()-startTime)+" ms");
			 long fl = fileLength;
			 fileLength = -1;
			return (int) fl;
		 }
	}
	else 
    {
    	BbyteLength = 0;
    	BbytePosition = 0;
		 int tmp = Decoding(Bbyte, 0);
		 if(tmp>0)
		 {
			 len = (len>BbyteLength)?BbyteLength:len;
			 //DFSClient.LOG.info("len: "+len+" buffersize: "+buf.length);//128K
			 System.arraycopy(Bbyte, BbytePosition, buf, off, len);
			 BbytePosition += len;
	    	 BbyteLength -= len;
	    	 if (fileLength>=len)
	    	 {
	    		 fileLength -= len;
	    		 //DFSClient.LOG.info("return 2 len: "+len);
	    		 return len;
	    	 }
	    	 else
	    	 {
				 //DFSClient.LOG.info("return 2 len: "+fileLength);
				 DFSClient.LOG.info("Cumulus read time consumption : " +(System.currentTimeMillis()-startTime)+"ms");
				 long fl = fileLength;
				 fileLength = -1;
				 return (int) fl;
	    	 }
		 }
		 else 
		 {
			return -1;
		 }
	 }
    
  }
  
  /*
   * TODO: DN failure must be added in the future
   */
  private int Decoding(byte[] buf,int off) throws IOException{
	   byte[][] decodebuf = new byte[k_blocks][packetSize];//data to be decoded
	   int[] dataLen = new int[k_blocks];//real dataLen, not length of buffer
	   int count = 0;
	   int len = 0;
	   byte[][] g = new byte[k_blocks][k_blocks];
	   short[][] G = new short[k_blocks][k_blocks];
	   //DFSClient.LOG.info("usingDN: "+usingDN.toString());
	   
	   Vector<Integer> fail = new Vector<Integer>();
	   for (int i = 0; i < k_blocks; i++) {	
			dataLen[count] = dps.elementAt(i).getData(decodebuf[count], packetSize);
			if (dps.elementAt(i).error){
				fail.add(i);
			}	
			else {
				usingDN[count]=usingDN[i];
				count++;
			}
		}	   
	 
	   //DFSClient.LOG.info("in Decoding after reading usingDN:"+usingDN.toString()+" fail size: "+fail.size());
	   //DFSClient.LOG.info("count:  "+count);	
	   //what if all k DNs failed........
	  	if(count!=0){
	  		if(count!=k_blocks){
	  		//error handling 
	  		//one DN fails then connect one of n-k DN
	  			if (n-blockIndex < fail.size()) {
	  				//no enough amount good blks to recover original data
					return -1;
				}
	  			for (int i = 0; i < fail.size(); i++) {
					dps.remove(fail.get(i));
				}
		  		while(count<k_blocks && blockIndex<n){
			  		   LocatedBlock block = locatedBlocks.getLocatedBlocks().get(blockIndex);
			  		   DNAddrPair retval = chooseDataNode(block);
			  	       //DatanodeInfo chosenNode = retval.info;
			  	       InetSocketAddress targetAddr = retval.addr;
			  	       BlockReader reader = null;
			  	     
			  	      // DFSClient.LOG.info("read block "+block.toString()+"........."+buffersize);
			  	       try {
			  		          Token<BlockTokenIdentifier> blockToken = block.getBlockToken();
			  		         // DFSClient.LOG.info(block.getStartOffset()+" blocksize:"+block.getBlockSize());	         
			  		          reader = getBlockReader(targetAddr, src,
			  		                                  block.getBlock(),
			  		                                  blockToken,
			  		                                  dps.firstElement().readedPosition, block.getBlockSize(), buffersize,
			  		                                  verifyChecksum, dfsClient.clientName);
			  		          dps.add(new DataPool(reader, dps.firstElement().readedPosition, 2*1024*1024));
			  		          dataLen[count] = dps.lastElement().getData(decodebuf[count], packetSize);
			  		          if (dataLen[count]!=-1){
				  					usingDN[count]=blockIndex;
				  					count++;
			  		          }				  		          
			  	       	}
			  	       catch (Exception e) {
			  				// TODO: handle exception
			  	       	//DFSClient.LOG.info(e.toString());
			  			}
			  	       blockIndex++;
		  		}
		  		if (count<k_blocks){
		  			throw new IOException("file has failed..............czl");
		  		}
	  			
	  		}
	  		//DFSClient.LOG.info("before decoding usingDN: "+usingDN.toString());
	  	// seq READ.1 5
			// modified by ds at 2014-5-9
			// modified modified by ds begins
			if (RegeneratingCodeMatrix.isRegeneratingCodeRecovery())
			{
				short[][] VdcMatrixInshort = getVdcMatrixInShort();
				len = matrix.decoder(VdcMatrixInshort, decodebuf, dataLen, off, buf);
			}
			else
			{
				byte[][] gg = matrix.getCodingmatrix();
				for (int i = 0; i < k_nodes; i++)
				{
					for (int j = 0; j < k_nodes; j++)
					{
						g[i][j] = gg[i][usingDN[j]];
					}
				}
				// DFSClient.LOG.info("g: "+g.toString());

				for (int i = 0; i < k_nodes; i++)
					for (int j = 0; j < k_nodes; j++)
					{
						if (g[j][i] < 0)
							G[j][i] = (short) (g[j][i] + 256);
						else
						{
							G[j][i] = (short) (g[j][i]);
						}
					}
				// len = RSDecoder(G, decodebuf,dataLen,off,buf);
				// len = XORDecoder(G, decodebuf,dataLen,off,buf);
				len = matrix.decoder(G, decodebuf, dataLen, off, buf);
			}
			// modified by ds ends
		  	pos += len;
		  	 BbyteLength += len;
		  	 //DFSClient.LOG.info("xy..........len:"+len);
		  	 return len;
	  	}
	  	return -1;
//		DFSClient.LOG.info("datalen: "+dataLen[1]);
//		if(dataLen[1]!=-1)
//	  	System.arraycopy(decodebuf[1], 0, buf, off, dataLen[1]);
//	  	return dataLen[1];
	  //	return -1;//return length of data that has been decoded
      
     
//      int count = 0;
//      File file = new File(blockIndex+" "+"zzzz");
//      FileOutputStream fo = new FileOutputStream(file);
//      while(count<block.getBlockSize()){
//    	  		int tmp = dps[blockIndex].getData(buf, buf.length);
//    	  		DFSClient.LOG.info("index: "+blockIndex+" temp: "+tmp);
//    	  		if (tmp == -1){
//    	  			if (count<block.getBlockSize()){
//    	  				throw new Exception("EOF, but no enough data!!!");
//    	  			}
//    	  			else {// read success finish
//						fo.flush();
//						fo.close();
//						break;
//					}
//    		      
//    	  		}
//    	  		count += tmp;
//    	  		fo.write(buf, 0, tmp);  		
//         }
  }
  
  
  
  /*
   * store decoded data into buf from offset 
   */
//  /**
//   * using Xor Decoder
//   * revised by zdy @ 2013/10/28
//   */
// private int XORDecoder(short[][] g,byte[][] Buf,int[] buflen,int offset,byte[] buf) throws IOException{
//    //g是编码矩阵，Buf是用于解码的数据，buflen对应Buf里数据的长度，解码得到的数据放到buf中从offset开始的位置
//    //返回的是buf中写入的长度
//    int len = 0;
//    int off = offset;
//    g = rsp.InitialInvertedCauchyMatrix(g);
//
//    for(int t = 0; t < k;t++){
//      len = 0;
//      for(int j = 0;j < k;j++){
//        if(g[j][t] != 0){
//          if(buflen[j] != -1 && len < buflen[j])
//            len = buflen[j];
//        }
//      }
//      short[] Output = new short[len];
//      Arrays.fill(Output,(short)0);
//      for(int i=0;i < k;i++){
//        if(g[i][t] != 0){
//          for(int j = 0;j < buflen[i];j++)
//            Output[j] = (short) (Output[j] ^ Buf[i][j]);
//        }
//      }
//
//      for(int i = 0 ;i < Output.length;i++)
//        buf[off++] = (byte)Output[i];
//    }
//
//    return (off - offset);
//  }

  /*
  private  int RSDecoder(short[][] g,byte[][] Buf,int[] buflen,int offset,byte[] buf) throws IOException{	
		int len = 0;
		int off = offset;
	   short[] Input; 
		short[] Output; 
		String s="\n";
		 for(int j=0;j<k;j++){
	    	 for(int i=0;i < k;i++){
	    		 s+=g[j][i]+" ";
	    		
	    	 }
	    	 s+="\n";
		 }
		// DFSClient.LOG.info("  matrix g:"+s);
		g=rsp.InitialInvertedCauchyMatrix(g);
//		 s="Inverted Matrix\n";
//		 for(int j=0;j<k;j++){
//	    	 for(int i=0;i < k;i++){
//	    		 s+=g[j][i]+" ";
//	    		
//	    	 }
//	    	 s+="\n";
//		 }
		// DFSClient.LOG.info("  matrix g:"+s);
	   for(int t = 0;t < k;t++){
		   len = 0;
		   for(int j = 0; j < k; j++)
			 {
				  if(g[j][t] != 0)
					  if(buflen[j] != -1 && len < buflen[j])
				      {					  
						   len = buflen[j];
				      }
			  }		   
		   Input = new short[len]; 
		   Output = new short[len]; 
		   
			  for(int i = 0;i < Output.length;i++){
				  Output[i]=(short)0;				  
			  }
			  
			  for (int i = 0; i < k; i++){	
					  if(g[i][t] != 0){
						  for (int p = 0; p < buflen[i]; p++)
						  {
							  if (Buf[i][p] < 0)
								  Input[p] = (short)(Buf[i][p] + 256);
							  else 
								  Input[p] = (short)Buf[i][p];
						  }
						  for(int j = 0;j < buflen[i];j++)
						  {
							  	Output[j]^= RSCoderProtocol.mult[Input[j]][g[i][t]];
						  }
					
					  }				  
			  }
			  
			  for(int i = 0;i < Output.length;i++){
				  buf[off++]=(byte)Output[i];				  
			  }
    
		  }
		// DFSClient.LOG.info("offsetinfile:"+off);
	   return (off - offset);
	 
  }
 
  private void RSDecoder(short[][] g,byte[][] Buf,byte[] tempbuf){
	RSCoderProtocol.setup_tables();
	RSCoderProtocol.CalculateValue();	
	short[] InputBytes=new short[(int) blockSize]; 
    short[] OutputBytes =new short[(int) blockSize]; 
		int count=0;
		String s="\n";
		 for(int j=0;j<row;j++){
	    	 for(int k=0;k<row;k++){
	    		 
	    		 if(g[j][k]<0)
	    			 g[j][k]= (short) (g[j][k]+256);
	    		 s+=g[j][k]+" ";
	    		
	    	 }
	    	 s+="\n";
		 }
		 dfsClient.LOG.info("  matrix g:"+s);
	   g=RSCoderProtocol.InitialInvertedCauchyMatrix(g);
	   
	   s="\n";
	  for(int j=0;j<row;j++){
	    	 for(int k=0;k<row;k++){
	    		 
	    		 if(g[j][k]<0)
	    			 g[j][k]= (short) (g[j][k]+256);
	    		 s+=g[j][k]+" ";
	    		
	    	 }
	    	 s+="\n";
		 }
	 dfsClient.LOG.info("inverted matrix g:"+s);
//	  for(int j=0;j<row;j++)
//	    	 for(int k=0;k<row;k++)
//	    		 dfsClient.LOG.info("inverted matrix g["+j+"]["+k+"]:"+g[j][k]);
	   for(int t=0;t<row;t++){
			  for(int i=0;i<OutputBytes.length;i++){
				  OutputBytes[i]=(short)0;
				  
			  }
			  for (int i = 0; i <row; i++){
				  for (int k = 0; k < blocksize; k++)
					  if (Buf[i][k] < 0)
						  InputBytes[k] = (short)(Buf[i][k] + 256);
					  else 
						  InputBytes[k] = (short)Buf[i][k];
				  for(int j=0;j<InputBytes.length;j++)
					  	OutputBytes[j]^= RSCoderProtocol.mult[InputBytes[j]][g[i][t]]; 
			  } 
			  for(int i=0;i<OutputBytes.length;i++){
				  tempbuf[count++]=(byte)OutputBytes[i];
			  }
			 
		        try {
		        	 DataOutputStream fout = new DataOutputStream(new FileOutputStream("/home/hadoop/block"+t));
					fout.write(tempbuf, (int)(t*blocksize),(int) blocksize );
					  fout.flush();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		      
		  }
   }
*/
     
//seq READ.1 3
	/***
	 * this method is to getDCMatrix. created at 2014-5-9
	 * 
	 * @author ds
	 */
	private short[][] getVdcMatrixInShort()
	{
		byte[][] VMatrix = matrix.getVandermondeMatrix();
		int rowNum = k_nodes;
		int columnNum = VMatrix[0].length;
		byte[][] VdcMatrix = new byte[rowNum][columnNum];
		for (int row = 0; row < rowNum; row++)
		{
			for (int column = 0; column < columnNum; column++)
			{
				VdcMatrix[row][column] = VMatrix[usingDN[row * columnNum]][column];
			}
		}
		short[][] VdcMatrixInShort = new short[rowNum][columnNum];
		for (int row = 0; row < rowNum; row++)
		{
			for (int column = 0; column < columnNum; column++)
			{
				byte tmp = VdcMatrix[row][column];
				VdcMatrixInShort[row][column] = tmp >= 0 ? (short) tmp : (short) (tmp + 256);
			}
		}
		return VdcMatrixInShort;
	}
  
  private DNAddrPair chooseDataNode(LocatedBlock block)
    throws IOException {
    while (true) {
      DatanodeInfo[] nodes = block.getLocations();
      try {
        DatanodeInfo chosenNode = bestNode(nodes, deadNodes);
        InetSocketAddress targetAddr = 
                          NetUtils.createSocketAddr(chosenNode.getName());
        return new DNAddrPair(chosenNode, targetAddr);
      } catch (IOException ie) {
        String blockInfo = block.getBlock() + " file=" + src;
        if (failures >= dfsClient.getMaxBlockAcquireFailures()) {
          throw new BlockMissingException(src, "Could not obtain block: " + blockInfo,
                                          block.getStartOffset());
        }
        
        if (nodes == null || nodes.length == 0) {
          DFSClient.LOG.info("No node available for block: " + blockInfo);
        }
        DFSClient.LOG.info("Could not obtain block " + block.getBlock()
            + " from any node: " + ie
            + ". Will get new block locations from namenode and retry...");
        try {
          // Introducing a random factor to the wait time before another retry.
          // The wait time is dependent on # of failures and a random factor.
          // At the first time of getting a BlockMissingException, the wait time
          // is a random number between 0..3000 ms. If the first retry
          // still fails, we will wait 3000 ms grace period before the 2nd retry.
          // Also at the second retry, the waiting window is expanded to 6000 ms
          // alleviating the request rate from the server. Similarly the 3rd retry
          // will wait 6000ms grace period before retry and the waiting window is
          // expanded to 9000ms. 
          double waitTime = timeWindow * failures +       // grace period for the last round of attempt
            timeWindow * (failures + 1) * dfsClient.r.nextDouble(); // expanding time window for each failure
          DFSClient.LOG.warn("DFS chooseDataNode: got # " + (failures + 1) + " IOException, will wait for " + waitTime + " msec.");
          Thread.sleep((long)waitTime);
        } catch (InterruptedException iex) {
        }
        deadNodes.clear(); //2nd option is to remove only nodes[blockId]
        openInfo();
        block = getBlockAt(block.getStartOffset(), false);
        failures++;
        continue;
      }
    }
  } 
      
  private void fetchBlockByteRange(LocatedBlock block, long start,
                                   long end, byte[] buf, int offset) throws IOException {
    //
    // Connect to best DataNode for desired Block, with potential offset
    //
    int refetchToken = 1; // only need to get a new access token once
    
    while (true) {
      // cached block locations may have been updated by chooseDataNode()
      // or fetchBlockAt(). Always get the latest list of locations at the 
      // start of the loop.
      block = getBlockAt(block.getStartOffset(), false);
      DNAddrPair retval = chooseDataNode(block);
      DatanodeInfo chosenNode = retval.info;
      InetSocketAddress targetAddr = retval.addr;
      BlockReader reader = null;
          
      try {
        Token<BlockTokenIdentifier> blockToken = block.getBlockToken();
            
        int len = (int) (end - start + 1);

        reader = getBlockReader(targetAddr, src,
                                block.getBlock(),
                                blockToken,
                                start, len, buffersize,
                                verifyChecksum, dfsClient.clientName);
        int nread = reader.readAll(buf, offset, len);
        if (nread != len) {
          throw new IOException("truncated return from reader.read(): " +
                                "excpected " + len + ", got " + nread);
        }
        return;
      } catch (ChecksumException e) {
        DFSClient.LOG.warn("fetchBlockByteRange(). Got a checksum exception for " +
                 src + " at " + block.getBlock() + ":" + 
                 e.getPos() + " from " + chosenNode.getName());
        dfsClient.reportChecksumFailure(src, block.getBlock(), chosenNode);
      } catch (IOException e) {
        if (e instanceof InvalidBlockTokenException && refetchToken > 0) {
          DFSClient.LOG.info("Will get a new access token and retry, "
              + "access token was invalid when connecting to " + targetAddr
              + " : " + e);
          refetchToken--;
          fetchBlockAt(block.getStartOffset());
          continue;
        } else {
          DFSClient.LOG.warn("Failed to connect to " + targetAddr + " for file " + src
              + " for block " + block.getBlock() + ":"
              + StringUtils.stringifyException(e));
        }
      } finally {
        if (reader != null) {
          closeBlockReader(reader);
        }
      }
      // Put chosen node into dead list, continue
      addToDeadNodes(chosenNode);
    }
  }

  /**
   * Close the given BlockReader and cache its socket.
   */
  private void closeBlockReader(BlockReader reader) throws IOException {
    if (reader.hasSentStatusCode()) {
      Socket oldSock = reader.takeSocket();
      socketCache.put(oldSock);
    }
    reader.close();
  }

  /**
   * Retrieve a BlockReader suitable for reading.
   * This method will reuse the cached connection to the DN if appropriate.
   * Otherwise, it will create a new connection.
   *
   * @param dnAddr  Address of the datanode
   * @param file  File location
   * @param block  The Block object
   * @param blockToken  The access token for security
   * @param startOffset  The read offset, relative to block head
   * @param len  The number of bytes to read
   * @param bufferSize  The IO buffer size (not the client buffer size)
   * @param verifyChecksum  Whether to verify checksum
   * @param clientName  Client name
   * @return New BlockReader instance
   */
  protected BlockReader getBlockReader(InetSocketAddress dnAddr,
                                       String file,
                                       Block block,
                                       Token<BlockTokenIdentifier> blockToken,
                                       long startOffset,
                                       long len,
                                       int bufferSize,
                                       boolean verifyChecksum,
                                       String clientName)
      throws IOException {
    IOException err = null;
    boolean fromCache = true;

    // Allow retry since there is no way of knowing whether the cached socket
    // is good until we actually use it.
    for (int retries = 0; retries <= nCachedConnRetry && fromCache; ++retries) {
      Socket sock = socketCache.get(dnAddr);
      if (sock == null) {
        fromCache = false;

        sock = dfsClient.socketFactory.createSocket();
        
        // TCP_NODELAY is crucial here because of bad interactions between
        // Nagle's Algorithm and Delayed ACKs. With connection keepalive
        // between the client and DN, the conversation looks like:
        //   1. Client -> DN: Read block X
        //   2. DN -> Client: data for block X
        //   3. Client -> DN: Status OK (successful read)
        //   4. Client -> DN: Read block Y
        // The fact that step #3 and #4 are both in the client->DN direction
        // triggers Nagling. If the DN is using delayed ACKs, this results
        // in a delay of 40ms or more.
        //
        // TCP_NODELAY disables nagling and thus avoids this performance
        // disaster.
        sock.setTcpNoDelay(true);

        NetUtils.connect(sock, dnAddr, dfsClient.socketTimeout);
        sock.setSoTimeout(dfsClient.socketTimeout);
      }

      try {
        // The OP_READ_BLOCK request is sent as we make the BlockReader
        BlockReader reader =
            BlockReader.newBlockReader(sock, file, block,
                                       blockToken,
                                       startOffset, len,
                                       bufferSize, verifyChecksum,
                                       clientName);
        return reader;
      } catch (IOException ex) {
        // Our socket is no good.
        DFSClient.LOG.debug("Error making BlockReader. Closing stale " + sock, ex);
        sock.close();
        err = ex;
      }
    }

    throw err;
  }


  /**
   * Read bytes starting from the specified position.
   * 
   * @param position start read from this position
   * @param buffer read buffer
   * @param offset offset into buffer
   * @param length number of bytes to read
   * 
   * @return actual number of bytes read
   */
  @Override
  public int read(long position, byte[] buffer, int offset, int length)
    throws IOException {
    // sanity checks
    dfsClient.checkOpen();
    if (closed) {
      throw new IOException("Stream closed");
    }
    /*
    failures = 0;
    long filelen = getFileLength();
    if ((position < 0) || (position >= filelen)) {
      return -1;
    }
    int realLen = length;
    if ((position + length) > filelen) {
      realLen = (int)(filelen - position);
    }
    
    // determine the block and byte range within the block
    // corresponding to position and realLen
    List<LocatedBlock> blockRange = getBlockRange(position, realLen);
    int remaining = realLen;
    for (LocatedBlock blk : blockRange) {
      long targetStart = position - blk.getStartOffset();
      long bytesToRead = Math.min(remaining, blk.getBlockSize() - targetStart);
      fetchBlockByteRange(blk, targetStart, 
                          targetStart + bytesToRead - 1, buffer, offset);
      remaining -= bytesToRead;
      position += bytesToRead;
      offset += bytesToRead;
    }
    assert remaining == 0 : "Wrong number of bytes read.";
    if (dfsClient.stats != null) {
      dfsClient.stats.incrementBytesRead(realLen);
    }
    return realLen;
  }
   
  @Override
  public long skip(long n) throws IOException {
    if ( n > 0 ) {
      long curPos = getPos();
      long fileLen = getFileLength();
      if( n+curPos > fileLen ) {
        n = fileLen - curPos;
      }
      seek(curPos+n);
      return n;
    }
    return n < 0 ? -1 : 0;
    */
    
    if (position >= length) return -1;
    
    int packetSize = 65024;
    if (position>pos){
	    long startOffset = position/(packetSize*k_blocks)*packetSize;
	    for (int i = 0; i < dps.size(); i++) {
	    	//each blk seek to specific position
			dps.elementAt(i).seekTo(startOffset);
		}
	    int partial = (int) (position%(packetSize*k_blocks));
	    while(partial>0){
	    	byte[] buf = new byte[buffer.length];
	    	int len = read(buf, 0, buffer.length);
	    	if (partial < len){
	    		System.arraycopy(buf, partial, buffer, offset, len-partial);
	    		pos = position + (len-partial);
	    		return len-partial;
	    	}
	    	else {
				partial -= len;
			}
	    }    
    }
    else{
    	if (position == pos) {
			return read(buffer,offset,length);
		}
    	
    }
    return -1;
    
  }

  /**
   * Seek to a new arbitrary location
   */
  @Override
  public synchronized void seek(long targetPos) throws IOException {
    if (targetPos > getFileLength()) {
      throw new IOException("Cannot seek after EOF");
    }
    if (closed) {
      throw new IOException("Stream is closed!");
    }
    boolean done = false;
    if (pos <= targetPos && targetPos <= blockEnd) {
      //
      // If this seek is to a positive position in the current
      // block, and this piece of data might already be lying in
      // the TCP buffer, then just eat up the intervening data.
      //
      int diff = (int)(targetPos - pos);
      if (diff <= DFSClient.TCP_WINDOW_SIZE) {
        try {
          pos += blockReader.skip(diff);
          if (pos == targetPos) {
            done = true;
          }
        } catch (IOException e) {//make following read to retry
          if(DFSClient.LOG.isDebugEnabled()) {
            DFSClient.LOG.debug("Exception while seek to " + targetPos +
                " from " + currentBlock +" of " + src + " from " +
                currentNode + ": " + StringUtils.stringifyException(e));
          }
        }
      }
    }
    if (!done) {
      pos = targetPos;
      blockEnd = -1;
    }
  }

  /**
   * Same as {@link #seekToNewSource(long)} except that it does not exclude
   * the current datanode and might connect to the same node.
   */
//  private synchronized boolean seekToBlockSource(long targetPos)
//                                                 throws IOException {
//    currentNode = blockSeekTo(targetPos);
//    return true;
//  }
  
  /**
   * Seek to given position on a node other than the current node.  If
   * a node other than the current node is found, then returns true. 
   * If another node could not be found, then returns false.
   */
  @Override
  public synchronized boolean seekToNewSource(long targetPos) throws IOException {
    boolean markedDead = deadNodes.containsKey(currentNode);
    addToDeadNodes(currentNode);
    DatanodeInfo oldNode = currentNode;
    DatanodeInfo newNode = blockSeekTo(targetPos);
    if (!markedDead) {
      /* remove it from deadNodes. blockSeekTo could have cleared 
       * deadNodes and added currentNode again. Thats ok. */
      deadNodes.remove(oldNode);
    }
    if (!oldNode.getStorageID().equals(newNode.getStorageID())) {
      currentNode = newNode;
      return true;
    } else {
      return false;
    }
  }
      
  /**
   */
  @Override
  public synchronized long getPos() throws IOException {
    return pos;
  }

  /** Return the size of the remaining available bytes
   * if the size is less than or equal to {@link Integer#MAX_VALUE},
   * otherwise, return {@link Integer#MAX_VALUE}.
   */
  @Override
  public synchronized int available() throws IOException {
    if (closed) {
      throw new IOException("Stream closed");
    }

    final long remaining = getFileLength() - pos;
    return remaining <= Integer.MAX_VALUE? (int)remaining: Integer.MAX_VALUE;
  }

  /**
   * We definitely don't support marks
   */
  @Override
  public boolean markSupported() {
    return false;
  }
  @Override
  public void mark(int readLimit) {
  }
  @Override
  public void reset() throws IOException {
    throw new IOException("Mark/reset not supported");
  }

  /**
   * Pick the best node from which to stream the data.
   * Entries in <i>nodes</i> are already in the priority order
   */
  static DatanodeInfo bestNode(DatanodeInfo nodes[], 
                               AbstractMap<DatanodeInfo, DatanodeInfo> deadNodes)
                               throws IOException {
    if (nodes != null) { 
      for (int i = 0; i < nodes.length; i++) {
        if (!deadNodes.containsKey(nodes[i])) {
          return nodes[i];
        }
      }
    }
    throw new IOException("No live nodes contain current block");
  }

  /** Utility class to encapsulate data node info and its ip address. */
  static class DNAddrPair {
    DatanodeInfo info;
    InetSocketAddress addr;
    DNAddrPair(DatanodeInfo info, InetSocketAddress addr) {
      this.info = info;
      this.addr = addr;
    }
  }

}