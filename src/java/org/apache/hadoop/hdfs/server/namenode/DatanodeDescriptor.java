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
package org.apache.hadoop.hdfs.server.namenode;

import java.io.DataInput;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.LinkedBlockingDeque;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocol.CodingMatrix;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.RegeneratingCodeMatrix;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs.BlockReportIterator;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.protocol.BlockCommand;
import org.apache.hadoop.hdfs.server.protocol.BlockRecoveryCommand;
import org.apache.hadoop.hdfs.server.protocol.CumulusRecoveryCommand;
import org.apache.hadoop.hdfs.server.protocol.RCRecoveryCommand;
import org.apache.hadoop.hdfs.server.protocol.BlockRecoveryCommand.RecoveringBlock;
import org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.hdfs.DeprecatedUTF8;
import org.apache.hadoop.io.WritableUtils;

import org.apache.hadoop.hdfs.server.monitor.*;//add by xianyu


/**************************************************
 * DatanodeDescriptor tracks stats on a given DataNode, such as
 * available storage capacity, last update time, etc., and maintains a
 * set of blocks stored on the datanode.
 *
 * This data structure is internal to the namenode. It is *not* sent
 * over-the-wire to the Client or the Datanodes. Neither is it stored
 * persistently in the fsImage.
 **************************************************/
@InterfaceAudience.Private
public class DatanodeDescriptor extends DatanodeInfo {
  
  // Stores status of decommissioning.
  // If node is not decommissioning, do not use this object for anything.
  DecommissioningStatus decommissioningStatus = new DecommissioningStatus();
  
  /** Block and targets pair */
  @InterfaceAudience.Private
  @InterfaceStability.Evolving
  public static class BlockTargetPair {
    public final Block block;
    public final DatanodeDescriptor[] targets;    

    BlockTargetPair(Block block, DatanodeDescriptor[] targets) {
      this.block = block;
      this.targets = targets;
    }
  }

  /** A BlockTargetPair queue. */
  private static class BlockQueue<E> {
    private final Queue<E> blockq = new LinkedList<E>();

    /** Size of the queue */
    synchronized int size() {return blockq.size();}

    /** Enqueue */
    synchronized boolean offer(E e) { 
      return blockq.offer(e);
    }

    /** Dequeue */
    synchronized List<E> poll(int numBlocks) {
      if (numBlocks <= 0 || blockq.isEmpty()) {
        return null;
      }

      List<E> results = new ArrayList<E>();
      for(; !blockq.isEmpty() && numBlocks > 0; numBlocks--) {
        results.add(blockq.poll());
      }
      return results;
    }

    /**
     * Returns <tt>true</tt> if the queue contains the specified element.
     */
    boolean contains(E e) {
      return blockq.contains(e);
    }
  }

  private volatile BlockInfo blockList = null;
  private int numBlocks = 0;
  // isAlive == heartbeats.contains(this)
  // This is an optimization, because contains takes O(n) time on Arraylist
  protected boolean isAlive = false;
  protected boolean needKeyUpdate = false;

  /** A queue of blocks to be replicated by this datanode */
  private BlockQueue<BlockTargetPair> replicateBlocks = new BlockQueue<BlockTargetPair>();
  /** A queue of blocks to be recovered by this datanode */
  private BlockQueue<BlockInfoUnderConstruction> recoverBlocks =
                                new BlockQueue<BlockInfoUnderConstruction>();
  /** A set of blocks to be invalidated by this datanode */
  private Set<Block> invalidateBlocks = new TreeSet<Block>();
  
  private Queue<BlockInfo> cumulusRecover = new LinkedBlockingDeque<BlockInfo>();
  
  /* Variables for maintaining number of blocks scheduled to be written to
   * this datanode. This count is approximate and might be slightly bigger
   * in case of errors (e.g. datanode does not report if an error occurs 
   * while writing the block).
   */
  private int currApproxBlocksScheduled = 0;
  private int prevApproxBlocksScheduled = 0;
  private long lastBlocksScheduledRollTime = 0;
  private static final int BLOCKS_SCHEDULED_ROLL_INTERVAL = 600*1000; //10min
  private int volumeFailures = 0;

  /** Default constructor */
  public DatanodeDescriptor() {}
  
  /** DatanodeDescriptor constructor
   * @param nodeID id of the data node
   */
  public DatanodeDescriptor(DatanodeID nodeID) {
	  /* add by xianyu
	  call: 
	    public DatanodeDescriptor(DatanodeID nodeID, 
                            long capacity,
                            long dfsUsed,
                            long remaining,
                            ServernodeCPUStatus cpuStatus, 
                            ServernodeMEMStatus memStatus, 
                            ServernodeNETStatus[] netStatus, 
                            ServernodeIOStatus[] ioStatus, 
                            int xceiverCount,
                            int failedVolumes)
	  */
	  this(nodeID, 0L, 0L, 0L, 
			  //add by xianyu
			  null, null, null, null, 
			  
			  //removed by xianyu
			  /*
			  0L, 0L, 0L, 
			  */
			  0, 0);  ////ww modified 
  }

  /** DatanodeDescriptor constructor
   * 
   * @param nodeID id of the data node
   * @param networkLocation location of the data node in network
   */
  public DatanodeDescriptor(DatanodeID nodeID, 
                            String networkLocation) {
    this(nodeID, networkLocation, null);
  }
  
  /** DatanodeDescriptor constructor
   * 
   * @param nodeID id of the data node
   * @param networkLocation location of the data node in network
   * @param hostName it could be different from host specified for DatanodeID
   */
  public DatanodeDescriptor(DatanodeID nodeID, 
                            String networkLocation,
                            String hostName) {
	  /* add by xianyu
	  call: 
	    public DatanodeDescriptor(DatanodeID nodeID,
                            String networkLocation,
                            String hostName,
                            long capacity,
                            long dfsUsed,
                            long remaining,
                            ServernodeCPUStatus cpuStatus, 
                            ServernodeMEMStatus memStatus, 
                            ServernodeNETStatus[] netStatus, 
                            ServernodeIOStatus[] ioStatus,
                            int xceiverCount,
                            int failedVolumes)
	  */
	  this(nodeID, networkLocation, hostName, 0L, 0L, 0L, 
			  //add by xianyu
			  null, null, null, null, 
			  
			  //removed by xianyu
			  /*
			  0L, 0L, 0L, 
			  */
			  0, 0); ////ww modified
  }
  
  /** DatanodeDescriptor constructor
   * 
   * @param nodeID id of the data node
   * @param capacity capacity of the data node
   * @param dfsUsed space used by the data node
   * @param remaining remaing capacity of the data node
   * @param xceiverCount # of data transfers at the data node
   */
  public DatanodeDescriptor(DatanodeID nodeID, 
                            long capacity,
                            long dfsUsed,
                            long remaining,
                            //add by xianyu
                            ServernodeCPUStatus cpuStatus, 
                            ServernodeMEMStatus memStatus, 
                            ServernodeNETStatus[] netStatus, 
                            ServernodeIOStatus[] ioStatus, 
                            
                            //removed by xianyu
                            /*
                            long cpuUsed,         //ww added
                            long memUsed,         //ww added
                            long ioUsed,          //ww added
                            */
                            int xceiverCount,
                            int failedVolumes) {
    super(nodeID);
    updateHeartbeat(capacity, dfsUsed, remaining, 
    		//add by xianyu
    		cpuStatus, memStatus, netStatus, ioStatus, 
    		
    		//removed by xianyu
    		/*
    		cpuUsed, memUsed, ioUsed, 
    		*/
    		xceiverCount, failedVolumes);
  }

  /** DatanodeDescriptor constructor
   * 
   * @param nodeID id of the data node
   * @param networkLocation location of the data node in network
   * @param capacity capacity of the data node, including space used by non-dfs
   * @param dfsUsed the used space by dfs datanode
   * @param remaining remaining capacity of the data node
   * @param xceiverCount # of data transfers at the data node
   */
  public DatanodeDescriptor(DatanodeID nodeID,
                            String networkLocation,
                            String hostName,
                            long capacity,
                            long dfsUsed,
                            long remaining,
                            /******* add by xianyu *******/
                            ServernodeCPUStatus cpuStatus, 
                            ServernodeMEMStatus memStatus, 
                            ServernodeNETStatus[] netStatus, 
                            ServernodeIOStatus[] ioStatus,
                            /*****************************/
                            
                            //removed by xianyu
                            /*
                            long cpuUsed,    //ww added
                            long memUsed,
                            long ioUsed,
                            */
                            int xceiverCount,
                            int failedVolumes) {
    super(nodeID, networkLocation, hostName);
    updateHeartbeat(capacity, dfsUsed, remaining, 
    		//add by xianyu
    		cpuStatus, memStatus, netStatus, ioStatus, 
    		
    		//removed by xianyu
    		/*
    		cpuUsed, memUsed, ioUsed, 
    		*/
    		xceiverCount, failedVolumes);
  }

  /**
   * Add datanode to the block.
   * Add block to the head of the list of blocks belonging to the data-node.
   */
  boolean addBlock(BlockInfo b) {
    if(!b.addNode(this))
      return false;
    // add to the head of the data-node list
    blockList = b.listInsert(blockList, this);
    numBlocks++;
    return true;
  }
  
  /**
   * Remove block from the list of blocks belonging to the data-node.
   * Remove datanode from the block.
   */
  boolean removeBlock(BlockInfo b) {
    blockList = b.listRemove(blockList, this);
    if ( b.removeNode(this) ) {
      numBlocks--;
      return true;
    } else {
      return false;
    }
  }

  /**
   * Move block to the head of the list of blocks belonging to the data-node.
   */
  void moveBlockToHead(BlockInfo b) {
    blockList = b.listRemove(blockList, this);
    blockList = b.listInsert(blockList, this);
  }

  /**
   * Replace specified old block with a new one in the DataNodeDescriptor.
   * 
   * @param oldBlock - block to be replaced
   * @param newBlock - a replacement block
   * @return the new block
   */
  BlockInfo replaceBlock(BlockInfo oldBlock, BlockInfo newBlock) {
    boolean done = removeBlock(oldBlock);
    assert done : "Old block should belong to the data-node when replacing";
    done = addBlock(newBlock);
    assert done : "New block should not belong to the data-node when replacing";
    return newBlock;
  }

  void resetBlocks() {
    this.capacity = 0;
    this.remaining = 0;
    this.dfsUsed = 0;
    
    /*** add by xianyu ***/
    this.cpuStatus = null;
    this.memStatus = null;
    this.netStatus = null;
    this.ioStatus = null;
    /********************/
    
    //removed by xianyu
    /*
    this.cpuUsed = 0;              //ww added
    this.memUsed = 0;              //ww added
    this.ioUsed = 0;               //ww added
    */
    this.xceiverCount = 0;
    this.blockList = null;
    this.invalidateBlocks.clear();
    this.volumeFailures = 0;
  }

  public int numBlocks() {
    return numBlocks;
  }

  /**
   * Updates stats from datanode heartbeat.
   */
  void updateHeartbeat(long capacity, long dfsUsed, long remaining,
		  
		  /******* add by xianyu *******/
		  ServernodeCPUStatus cpuStatus, 
		  ServernodeMEMStatus memStatus, 
		  ServernodeNETStatus[] netStatus, 
		  ServernodeIOStatus[] ioStatus,
		  /*****************************/
		  
		  //removed by xianyu
		  /*
		  long cpuUsed, long memUsed, long ioUsed, 
		  */
		  int xceiverCount, int volFailures) {
    this.capacity = capacity;
    this.dfsUsed = dfsUsed;
    this.remaining = remaining;
    /****** add by xianyu ******/
    this.cpuStatus = cpuStatus;
    this.memStatus = memStatus;
    this.netStatus = netStatus;
    this.ioStatus = ioStatus;
    /***************************/
    
    //removed by xianyu
    /*
    this.cpuUsed = cpuUsed;                    //ww added
    this.memUsed = memUsed;                    //ww added
    this.ioUsed = ioUsed;                      //ww adde
    */
    this.lastUpdate = System.currentTimeMillis();
    this.xceiverCount = xceiverCount;
    this.volumeFailures = volFailures;
    rollBlocksScheduled(lastUpdate);
  }

  /**
   * Iterates over the list of blocks belonging to the datanode.
   */
  static private class BlockIterator implements Iterator<BlockInfo> {
    private BlockInfo current;
    private DatanodeDescriptor node;
      
    BlockIterator(BlockInfo head, DatanodeDescriptor dn) {
      this.current = head;
      this.node = dn;
    }

    public boolean hasNext() {
      return current != null;
    }

    public BlockInfo next() {
      BlockInfo res = current;
      current = current.getNext(current.findDatanode(node));
      return res;
    }

    public void remove()  {
      throw new UnsupportedOperationException("Sorry. can't remove.");
    }
  }

  Iterator<BlockInfo> getBlockIterator() {
    return new BlockIterator(this.blockList, this);
  }
  
  /**
   * Store block replication work.
   */
  void addBlockToBeReplicated(Block block, DatanodeDescriptor[] targets) {
    assert(block != null && targets != null && targets.length > 0);
    replicateBlocks.offer(new BlockTargetPair(block, targets));
  }
  
  
  /**
   * added by czl
   * add block to recover queue
   * @param block
   */
  void addBlockToBeRecoveredCumulus(BlockInfo block){
	   cumulusRecover.offer(block);
   }

  /**
   * Store block recovery work.
   */
  void addBlockToBeRecovered(BlockInfoUnderConstruction block) {
    if(recoverBlocks.contains(block)) {
      // this prevents adding the same block twice to the recovery queue
      FSNamesystem.LOG.info("Block " + block +
                            " is already in the recovery queue.");
      return;
    }
    recoverBlocks.offer(block);
  }

  /**
   * Store block invalidation work.
   */
  void addBlocksToBeInvalidated(List<Block> blocklist) {
    assert(blocklist != null && blocklist.size() > 0);
    synchronized (invalidateBlocks) {
      for(Block blk : blocklist) {
        invalidateBlocks.add(blk);
      }
    }
  }

  /**
   * The number of work items that are pending to be replicated
   */
  int getNumberOfBlocksToBeReplicated() {
    return replicateBlocks.size();
  }

  /**
   * The number of block invalidation items that are pending to 
   * be sent to the datanode
   */
  int getNumberOfBlocksToBeInvalidated() {
    synchronized (invalidateBlocks) {
      return invalidateBlocks.size();
    }
  }
  
  
  /************** add by xianyu ****************
  /**
   * The number of work items that are pending to be recovered
   */
  int getNumberOfBlocksToBeRecoved(){
	  return recoverBlocks.size();
  }
  
  /**
   * live or dead
   */
  boolean isAlive(){
	  return isAlive;
  }
  /***********************************************/
  
  
  /**
   * added by czl
   * @return
   */
  CumulusRecoveryCommand getCumulusRecoveryCommand(){
	  BlockInfo blockInfo = cumulusRecover.poll();
	  if (blockInfo == null) {
		return null;
	}
	  INodeFile filenode = blockInfo.getINode();
	  List<BlockTargetPair> blockTargetPairs = new ArrayList<BlockTargetPair>();
	  DatanodeDescriptor[] dnds;
	  byte lostColumn = 0;
	  byte i = 0;
	  for(BlockInfo blkInfo: filenode.getBlocks()){
	  	  dnds = new DatanodeDescriptor[1];
		  dnds[0] = blkInfo.getDatanode(0);
		  NameNode.LOG.info("hahahhhahah"+dnds[0].toString()+"    "+blkInfo.getBlockId());
		  if (blkInfo.getBlockId() == blockInfo.getBlockId()) {
			lostColumn = i;
		}
		  i++;
		  blockTargetPairs.add(new BlockTargetPair((Block)blkInfo, dnds));
	  }
	  CodingMatrix matrix = filenode.getMatrix();
	  FSNamesystem.LOG.info(lostColumn+"   .............    ");
	  return new CumulusRecoveryCommand(DatanodeProtocol.DNA_CUMULUS_RECOVERY, filenode.getType(), lostColumn, matrix, blockTargetPairs);
  }
  
//seq RCR_NN_APPOINTnc.2 1
	/**
	 * get rcr command, based on getCumulusRecoveryCommand(). created at 2014-4-21. modified at.
	 * 
	 * @author ds
	 * @return
	 */
	RCRecoveryCommand getRCRecoveryCommand()
	{
		BlockInfo representBlockInfo = cumulusRecover.poll();
		if (representBlockInfo == null)
		{
			return null;
		}
		INodeFile filenode = representBlockInfo.getINode();
		BlockInfo[] fileBlockInfos = filenode.getBlocks();

		byte failednodeRow = 0;
		int a = ((RegeneratingCodeMatrix) filenode.getMatrix()).getA();// 4-28
		LocatedBlock[] fileLocatedBlocks = new LocatedBlock[fileBlockInfos.length];
		for (int i = 0; i < fileBlockInfos.length; i++)
		{
			BlockInfo blockInfo = fileBlockInfos[i];
			if (blockInfo.getBlockId() == representBlockInfo.getBlockId())
			{
				failednodeRow = (byte) (i / a);
			}
			DatanodeInfo datanode = blockInfo.getDatanode(0);
			LocatedBlock locatedBlock = new LocatedBlock((Block) blockInfo, new DatanodeInfo[]
			{ datanode });
			fileLocatedBlocks[i] = locatedBlock;
		}
		CodingMatrix matrix = filenode.getMatrix();
		FSNamesystem.LOG.info("failednodeRow" + failednodeRow + "   .............    ");
		return new RCRecoveryCommand(DatanodeProtocol.DNA_RC_RECOVERY, filenode.getType(), failednodeRow, matrix,
				fileLocatedBlocks);
		// test27-1 exception object cannot casted to LocatedBlock
	}
  
  BlockCommand getReplicationCommand(int maxTransfers) {
    List<BlockTargetPair> blocktargetlist = replicateBlocks.poll(maxTransfers);
    return blocktargetlist == null? null:
        new BlockCommand(DatanodeProtocol.DNA_TRANSFER, blocktargetlist);
  }

  BlockRecoveryCommand getLeaseRecoveryCommand(int maxTransfers) {
    List<BlockInfoUnderConstruction> blocks = recoverBlocks.poll(maxTransfers);
    if(blocks == null)
      return null;
    BlockRecoveryCommand brCommand = new BlockRecoveryCommand(blocks.size());
    for(BlockInfoUnderConstruction b : blocks) {
      brCommand.add(new RecoveringBlock(
          b, b.getExpectedLocations(), b.getBlockRecoveryId()));
    }
    return brCommand;
  }

  /**
   * Remove the specified number of blocks to be invalidated
   */
  BlockCommand getInvalidateBlocks(int maxblocks) {
    Block[] deleteList = getBlockArray(invalidateBlocks, maxblocks); 
    return deleteList == null? 
        null: new BlockCommand(DatanodeProtocol.DNA_INVALIDATE, deleteList);
  }

  static private Block[] getBlockArray(Collection<Block> blocks, int max) {
    Block[] blockarray = null;
    synchronized(blocks) {
      int available = blocks.size();
      int n = available;
      if (max > 0 && n > 0) {
        if (max < n) {
          n = max;
        }
        // allocate the properly sized block array ... 
        blockarray = new Block[n];

        // iterate tree collecting n blocks... 
        Iterator<Block> e = blocks.iterator();
        int blockCount = 0;

        while (blockCount < n && e.hasNext()) {
          // insert into array ... 
          blockarray[blockCount++] = e.next();

          // remove from tree via iterator, if we are removing 
          // less than total available blocks
          if (n < available){
            e.remove();
          }
        }
        assert(blockarray.length == n);
        
        // now if the number of blocks removed equals available blocks,
        // them remove all blocks in one fell swoop via clear
        if (n == available) { 
          blocks.clear();
        }
      }
    }
    return blockarray;
  }

  void reportDiff(BlockManager blockManager,
                  BlockListAsLongs newReport,
                  Collection<Block> toAdd,    // add to DatanodeDescriptor
                  Collection<Block> toRemove, // remove from DatanodeDescriptor
                  Collection<Block> toInvalidate, // should be removed from DN
                  Collection<BlockInfo> toCorrupt) {// add to corrupt replicas
    // place a delimiter in the list which separates blocks 
    // that have been reported from those that have not
    BlockInfo delimiter = new BlockInfo(new Block(), 1);
    boolean added = this.addBlock(delimiter);
    assert added : "Delimiting block cannot be present in the node";
    if(newReport == null)
      newReport = new BlockListAsLongs();
    // scan the report and process newly reported blocks
    BlockReportIterator itBR = newReport.getBlockReportIterator();
    while(itBR.hasNext()) {
      Block iblk = itBR.next();
      ReplicaState iState = itBR.getCurrentReplicaState();
      BlockInfo storedBlock = processReportedBlock(blockManager, iblk, iState,
                                               toAdd, toInvalidate, toCorrupt);
      // move block to the head of the list
      if(storedBlock != null && storedBlock.findDatanode(this) >= 0)
        this.moveBlockToHead(storedBlock);
    }
    // collect blocks that have not been reported
    // all of them are next to the delimiter
    Iterator<? extends Block> it = new BlockIterator(delimiter.getNext(0),this);
    while(it.hasNext())
      toRemove.add(it.next());
    this.removeBlock(delimiter);
  }

  /**
   * Process a block replica reported by the data-node.
   * 
   * <ol>
   * <li>If the block is not known to the system (not in blocksMap) then the
   * data-node should be notified to invalidate this block.</li>
   * <li>If the reported replica is valid that is has the same generation stamp
   * and length as recorded on the name-node, then the replica location is
   * added to the name-node.</li>
   * <li>If the reported replica is not valid, then it is marked as corrupt,
   * which triggers replication of the existing valid replicas.
   * Corrupt replicas are removed from the system when the block
   * is fully replicated.</li>
   * </ol>
   * 
   * @param blockManager
   * @param block reported block replica
   * @param rState reported replica state
   * @param toAdd add to DatanodeDescriptor
   * @param toInvalidate missing blocks (not in the blocks map)
   *        should be removed from the data-node
   * @param toCorrupt replicas with unexpected length or generation stamp;
   *        add to corrupt replicas
   * @return
   */
  BlockInfo processReportedBlock(
                  BlockManager blockManager,
                  Block block,                // reported block replica
                  ReplicaState rState,        // reported replica state
                  Collection<Block> toAdd,    // add to DatanodeDescriptor
                  Collection<Block> toInvalidate, // should be removed from DN
                  Collection<BlockInfo> toCorrupt) {// add to corrupt replicas
    if(FSNamesystem.LOG.isDebugEnabled()) {
      FSNamesystem.LOG.debug("Reported block " + block
          + " on " + getName() + " size " + block.getNumBytes()
          + " replicaState = " + rState);
    }

    // find block by blockId
    BlockInfo storedBlock = blockManager.blocksMap.getStoredBlock(block);
    if(storedBlock == null) {
      // If blocksMap does not contain reported block id,
      // the replica should be removed from the data-node.
      toInvalidate.add(new Block(block));
      return null;
    }

    if(FSNamesystem.LOG.isDebugEnabled()) {
      FSNamesystem.LOG.debug("In memory blockUCState = " +
          storedBlock.getBlockUCState());
    }

    // Ignore replicas already scheduled to be removed from the DN
    if(blockManager.belongsToInvalidates(getStorageID(), block)) {
      assert storedBlock.findDatanode(this) < 0 : "Block " + block 
        + " in recentInvalidatesSet should not appear in DN " + this;
      return storedBlock;
    }

    // Block is on the DN
    boolean isCorrupt = false;
    switch(rState) {
    case FINALIZED:
      switch(storedBlock.getBlockUCState()) {
      case COMPLETE:
      case COMMITTED:
        if(storedBlock.getGenerationStamp() != block.getGenerationStamp()
            || storedBlock.getNumBytes() != block.getNumBytes())
          isCorrupt = true;
        break;
      case UNDER_CONSTRUCTION:
      case UNDER_RECOVERY:
        ((BlockInfoUnderConstruction)storedBlock).addReplicaIfNotPresent(
            this, block, rState);
      }
      if(!isCorrupt && storedBlock.findDatanode(this) < 0)
        if (storedBlock.getNumBytes() != block.getNumBytes()) {
          toAdd.add(new Block(block));
        } else {
          toAdd.add(storedBlock);
        }
      break;
    case RBW:
    case RWR:
      if(!storedBlock.isComplete())
        ((BlockInfoUnderConstruction)storedBlock).addReplicaIfNotPresent(
                                                      this, block, rState);
      else
        isCorrupt = true;
      break;
    case RUR:       // should not be reported
    case TEMPORARY: // should not be reported
    default:
      FSNamesystem.LOG.warn("Unexpected replica state " + rState
          + " for block: " + storedBlock + 
          " on " + getName() + " size " + storedBlock.getNumBytes());
      break;
    }
    if(isCorrupt)
        toCorrupt.add(storedBlock);
    return storedBlock;
  }

  /** Serialization for FSEditLog */
  void readFieldsFromFSEditLog(DataInput in) throws IOException {
    this.name = DeprecatedUTF8.readString(in);
    this.storageID = DeprecatedUTF8.readString(in);
    this.infoPort = in.readShort() & 0x0000ffff;

    this.capacity = in.readLong();
    this.dfsUsed = in.readLong();
    this.remaining = in.readLong();
    
    /************** add by xianyu *********************/
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
    /**************************************************/
    
    //removed by xianyu
    /*
    this.cpuUsed = in.readLong();                 //ww added
    this.memUsed = in.readLong();                 //ww added
    this.ioUsed = in.readLong();                  //ww added
    */
    this.lastUpdate = in.readLong();
    this.xceiverCount = in.readInt();
    this.location = Text.readString(in);
    this.hostName = Text.readString(in);
    setAdminState(WritableUtils.readEnum(in, AdminStates.class));
  }
  
  /**
   * @return Approximate number of blocks currently scheduled to be written 
   * to this datanode.
   */
  public int getBlocksScheduled() {
    return currApproxBlocksScheduled + prevApproxBlocksScheduled;
  }
  
  /**
   * Increments counter for number of blocks scheduled. 
   */
  void incBlocksScheduled() {
    currApproxBlocksScheduled++;
  }
  
  /**
   * Decrements counter for number of blocks scheduled.
   */
  void decBlocksScheduled() {
    if (prevApproxBlocksScheduled > 0) {
      prevApproxBlocksScheduled--;
    } else if (currApproxBlocksScheduled > 0) {
      currApproxBlocksScheduled--;
    } 
    // its ok if both counters are zero.
  }
  
  /**
   * Adjusts curr and prev number of blocks scheduled every few minutes.
   */
  private void rollBlocksScheduled(long now) {
    if ((now - lastBlocksScheduledRollTime) > 
        BLOCKS_SCHEDULED_ROLL_INTERVAL) {
      prevApproxBlocksScheduled = currApproxBlocksScheduled;
      currApproxBlocksScheduled = 0;
      lastBlocksScheduledRollTime = now;
    }
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
  
  class DecommissioningStatus {
    int underReplicatedBlocks;
    int decommissionOnlyReplicas;
    int underReplicatedInOpenFiles;
    long startTime;
    
    synchronized void set(int underRep,
        int onlyRep, int underConstruction) {
      if (isDecommissionInProgress() == false) {
        return;
      }
      underReplicatedBlocks = underRep;
      decommissionOnlyReplicas = onlyRep;
      underReplicatedInOpenFiles = underConstruction;
    }
    
    synchronized int getUnderReplicatedBlocks() {
      if (isDecommissionInProgress() == false) {
        return 0;
      }
      return underReplicatedBlocks;
    }
    synchronized int getDecommissionOnlyReplicas() {
      if (isDecommissionInProgress() == false) {
        return 0;
      }
      return decommissionOnlyReplicas;
    }

    synchronized int getUnderReplicatedInOpenFiles() {
      if (isDecommissionInProgress() == false) {
        return 0;
      }
      return underReplicatedInOpenFiles;
    }

    synchronized void setStartTime(long time) {
      startTime = time;
    }
    
    synchronized long getStartTime() {
      if (isDecommissionInProgress() == false) {
        return 0;
      }
      return startTime;
    }
  }  // End of class DecommissioningStatus

  /**
   * @return number of failed volumes in the datanode.
   */
  public int getVolumeFailures() {
    return volumeFailures;
  }

  /**
   * @param nodeReg DatanodeID to update registration for.
   */
  public void updateRegInfo(DatanodeID nodeReg) {
    super.updateRegInfo(nodeReg);
  }
}
