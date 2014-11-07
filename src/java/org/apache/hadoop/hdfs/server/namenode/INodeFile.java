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

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.CodingMatrix;

class INodeFile extends INode {
  static final FsPermission UMASK = FsPermission.createImmutable((short)0111);

  //Number of bits for Block size
//  static final short BLOCKBITS = 48;
  // offset is a better name
  static final int fileSizeStart = 0;
  static final int packetSizeStart = 6; 
  static final int matrixStart = 12; 
//seq LCTBLK.2 1
	// modified by ds at 2014-4-27
	////static final int headerSize = 128;
	static final int headerSize = 512;
  
  //added by czl
  static byte matrixType = 0;
  //Header mask 64-bit representation
  //Format: [16 bits for replication][48 bits for PreferredBlockSize]
  //  static final long HEADERMASK = 0xffffL << BLOCKBITS;

  protected byte[] header;
  
  /**add by tony**/
  protected HeaderBuffer headerbuffer=null;
  
  /**the position of the header in the storagefile**/
  public long header_offset = -1;
  
  protected BlockInfo blocks[] = null;
  
  
  /*modified by tony*/
  INodeFile(PermissionStatus permissions, CodingMatrix codingMatrix,
          long modificationTime,
          long atime, long fileSize, long packetSize,HeaderBuffer headerbuf) {
  this(permissions, codingMatrix, null, modificationTime, atime, fileSize, packetSize,headerbuf);
  }

  protected INodeFile() {
	    blocks = null;
	    header = null;
	    header_offset = -1;
	  }
  
  /**
   *  modified by tony
   *  new parameter:
   *  @param headerbuf
   */
 protected INodeFile(PermissionStatus permissions, CodingMatrix codingMatrix, BlockInfo[] blklist,
         long modificationTime, long atime, long fileSize, long packetSize,HeaderBuffer headerbuf) {
	super(permissions, modificationTime, atime);
	
	header = new byte[headerSize];
	
	/*****add by tony*****/
	this.setHeaderBuffer(headerbuf);
	this.setFileSize(fileSize);
	this.setPacketSize(packetSize);
	this.setMatrix(codingMatrix);
	
	this.header_offset = -1;
	
	blocks = blklist;
}

 /***
  * @author tony 
  * invoked when the INodeFile is loaded from FSImage
  * new parameters:
  * @param header_offset
  * @param headerbuf
  */
 protected INodeFile(PermissionStatus permissions, CodingMatrix codingMatrix, long header_offset,
		  			  BlockInfo[] blklist, long modificationTime, long atime, long fileSize, 
		  			  long packetSize,HeaderBuffer headerbuf)
 {
		super(permissions, modificationTime, atime);
		
		header = new byte[headerSize];
		this.setHeaderBuffer(headerbuf);
		
		this.setFileSize(fileSize);
		this.setPacketSize(packetSize);
		this.setMatrix(codingMatrix);
		
		this.header_offset = header_offset;
		
		blocks = blklist;
 }
 
 /***
  * @author tony
  * invoked in the INodeFileUnderConstruction.convertToInodeFile()
  */
 protected INodeFile(INodeFile oldfile)
 {
	  super(oldfile);
	  header = new byte[headerSize];
	  this.setHeaderBuffer(oldfile.getHeaderBuffer());
	  this.setFileSize(oldfile.getFileSize());
	  this.setPacketSize(oldfile.getPacketSize());
	  this.setMatrix(oldfile.getMatrix());
	  this.header_offset = oldfile.header_offset;
	  //changeHeader(oldfile);
	  this.blocks = oldfile.getBlocks();
 }
 
  /**
   * Set the {@link FsPermission} of this {@link INodeFile}.
   * Since this is a file,
   * the {@link FsAction#EXECUTE} action, if any, is ignored.
   */
  protected void setPermission(FsPermission permission) {
    super.setPermission(permission.applyUMask(UMASK));
  }

  public boolean isDirectory() {
    return false;
  }

  /**
   * Get block replication for the file 
   * @return block replication value
   */
  public short getReplication() {
    return 1;
  }
  
  
  public void setType(byte type){
	  this.matrixType = type;
  }
  
  public byte getType() {
	  return matrixType;
}
  /**
   * @author tony
   * serialize the header vector into the buffer
   * 
   */
   public void serializeHeader(){
	   NameNode.LOG.info("INodeFile serializeHeader "+this+" "+header+" "+this.headerbuffer+"--------TONY ");
	   
	   this.headerbuffer.put(this, header);
	   this.header = null;
   }
   
   /***
    * @author tony
    * remove the BufferData from the buffer
    */
   public void removeFromBuffer()
   {
	   NameNode.LOG.info("INodeFile removeFromBuffer "+this+" ------------TONY");
	   this.headerbuffer.remove(this);
   }
   
   /***
    *  @author tony
    *  change the oldfile in the buffer to the newfile 
    * @param oldfile
    */
   public void changeHeader(INodeFile oldfile)
   {
	   this.headerbuffer.change(oldfile,this);
	   this.header = null;
   }
   
   /** @author tony
    *  get the header vector from the buffer
    *  @param path  the absolute path of this file
    *  @return the header vector
    * */
   public byte[] fetchBufferData()
   {
	   NameNode.LOG.info("in fetchBufferData in INodeFile "+this+" -----------TONY");
	   BufferData ans=this.headerbuffer.get(this);
       NameNode.LOG.info("BufferData: "+ans.toString()+"----------TONY");
	   return ans.getHead();
   }
   
   /**
    *  @author tony 
    */
   public HeaderBuffer getHeaderBuffer()
   {
	   return this.headerbuffer;
   }
   
   /**
    * @author tony
    */
   @Override
   public int hashCode()
   {
	   return super.hashCode();
   }
   
   /**
    * @author tony
    */
   @Override
   public boolean equals(Object o) {
	    if (!(o instanceof INodeFile)) {
	      return false;
	    }
	    return this.getFullPathName().equals(((INodeFile)o).getFullPathName());
	}
   
   /**
    * @author tony
    * @param headerbuf
    */
   public void setHeaderBuffer(HeaderBuffer headerbuf)
   {
	   this.headerbuffer=headerbuf;
   }
   
  /**
   *  Modified by tony 
   * @return
   */
  public CodingMatrix getMatrix(){
	  /****************/
	  if(header==null)
	  {
		  NameNode.LOG.info("in getMatrix()-------------TONY");
		  header=fetchBufferData();
	  }
	  int cur = matrixStart;
	  byte row = header[cur++];
	  byte column = header[cur++];
	  CodingMatrix matrix = CodingMatrix.getMatrixofCertainType(matrixType);
	  for(int i = 0; i < row; ++i)
		  for(int j = 0; j < column; ++j){
			  matrix.setElemAt(i, j, header[cur++]);
		  }
	  return matrix;
  }
  
  public void setMatrix(CodingMatrix matrix) {
	  int cur = matrixStart;
	  header[cur++] = matrix.getRow();
	  header[cur++] = matrix.getColumn();
	  for(int i = 0; i < matrix.getRow(); ++i)
		  for(int j = 0; j < matrix.getColumn(); ++j){
			  header[cur++] = matrix.getElemAt(i, j);
		  }
	  
  }
  
//  public void setReplication(short replication) {
//    if(replication <= 0)
//       throw new IllegalArgumentException("Unexpected value for the replication");
//    header = ((long)replication << BLOCKBITS) | (header & ~HEADERMASK);
//  }

  /**Modified by tony
   * 
   * Get preferred block size for the file
   * @return preferred block size in bytes
   */
  public long getFileSize() {
	  /****************/
	  if(header==null)
	  {
		  NameNode.LOG.info("In getFileSize()-----------TONY");
		  header=fetchBufferData();
	  }
      long blockSize = 0;
      int byteOfBlockSize = packetSizeStart - fileSizeStart;
      for(int i = 0; i < byteOfBlockSize; i ++){
       	blockSize = blockSize | ((((long)header[i]<<56)>>>56)<<(40-8*i));
        }
      return blockSize;
  }

  public void setFileSize(long fileSize)
  {
	    int byteOfBlockSize = packetSizeStart - fileSizeStart;
	    for (int i = 0; i < byteOfBlockSize; i++) {
			 header[i] = (byte)((fileSize << (16+8*i)) >>> 56);
		  }
  }
  /**
   *  Modified by tony
   * @return
   */
  public long getPacketSize(){
	   /****************/
	   if(header==null)
	   {
		   NameNode.LOG.info("In getPacketSize()----------TONY");
		   header=fetchBufferData();
	   }
	   long size = 0;
	   for(int cur = packetSizeStart; cur < matrixStart; ++cur) {
		   size |=  ((((long)header[cur])<<56)>>>(cur - packetSizeStart + 2) * 8);
	   }
	   return size;
	   
  }
  public void setPacketSize(long size) {
	  int index = packetSizeStart;
	  header[index++] = (byte)(size>>40);
	  header[index++] = (byte)(size>>32);
	  header[index++] = (byte)(size>>24);
	  header[index++] = (byte)(size>>16);
	  header[index++] = (byte)(size>>8);
	  header[index++] = (byte)(size);
}

  /**
   * Get file blocks 
   * @return file blocks
   */
  BlockInfo[] getBlocks() {
	  return this.blocks;
  }
//  <T extends BlockInfo> T[] getBlockInfo() throws IOException{
//	    if (blocks == null || blocks.length == 0)
//	        return null;
//	    T[] ret = null;
//	      try {
//	        @SuppressWarnings("unchecked")  // ClassCastException is caught below
//	        T[] tmp = (T[])blocks;
//	        ret = tmp;
//	      } catch(ClassCastException cce) {
//	        throw new IOException("Unexpected last block type: " 
//	            + blocks[blocks.length - 1].getClass().getSimpleName());
//	      }
//	      return ret;
//  }

  /**
   * append array of blocks to this.blocks
   */
  void appendBlocks(INodeFile [] inodes, int totalAddedBlocks) {
    int size = this.blocks.length;
    
    BlockInfo[] newlist = new BlockInfo[size + totalAddedBlocks];
    System.arraycopy(this.blocks, 0, newlist, 0, size);
    
    for(INodeFile in: inodes) {
      System.arraycopy(in.blocks, 0, newlist, size, in.blocks.length);
      size += in.blocks.length;
    }
    
    for(BlockInfo bi: this.blocks) {
      bi.setINode(this);
    }
    this.blocks = newlist;
  }
  
  /**
   * add a block to the block list
   */
  void addBlock(BlockInfo newblock) {
    if (this.blocks == null) {
      this.blocks = new BlockInfo[1];
      this.blocks[0] = newblock;
    } else {
      int size = this.blocks.length;
      BlockInfo[] newlist = new BlockInfo[size + 1];
      System.arraycopy(this.blocks, 0, newlist, 0, size);
      newlist[size] = newblock;
      this.blocks = newlist;
    }
  }

  /**
   * Set file block
   */
  void setBlock(int idx, BlockInfo blk) {
    this.blocks[idx] = blk;
  }

  int collectSubtreeBlocksAndClear(List<Block> v) {
    parent = null;
    if(blocks != null && v != null) {
      for (BlockInfo blk : blocks) {
        v.add(blk);
        blk.setINode(null);
      }
    }
    blocks = null;
    return 1;
  }

  /** {@inheritDoc} */
  long[] computeContentSummary(long[] summary) {
    summary[0] += computeFileSize(true);
    summary[1]++;
    summary[3] += diskspaceConsumed();
    return summary;
  }

  /** Compute file size.
   * May or may not include BlockInfoUnderConstruction.
   */
  long computeFileSize(boolean includesBlockInfoUnderConstruction) {
//    if (blocks == null || blocks.length == 0) {
//      return 0;
//    }
//    final int last = blocks.length - 1;
//    //check if the last block is BlockInfoUnderConstruction
//    long bytes = blocks[last] instanceof BlockInfoUnderConstruction
//                 && !includesBlockInfoUnderConstruction?
//                     0: blocks[last].getNumBytes();
//    for(int i = 0; i < last; i++) {
//      bytes += blocks[i].getNumBytes();
//    }
//    return bytes;
	  return getFileSize();
  }
  

  @Override
  DirCounts spaceConsumedInTree(DirCounts counts) {
    counts.nsCount += 1;
    counts.dsCount += diskspaceConsumed();
    return counts;
  }

  long diskspaceConsumed() {
    return diskspaceConsumed(blocks);
  }
  
  long diskspaceConsumed(Block[] blkArr) {
    long size = 0;
    if(blkArr == null) 
      return 0;
    
    for (Block blk : blkArr) {
      if (blk != null) {
        size += blk.getNumBytes();
      }
    }
    /* If the last block is being written to, use prefferedBlockSize
     * rather than the actual block size.
     */
//    if (blkArr.length > 0 && blkArr[blkArr.length-1] != null && 
//        isUnderConstruction()) {
//      size += getPreferredBlockSize() - blkArr[blkArr.length-1].getNumBytes();
//    }
    return size;
  }
  
  /**
   * Return the penultimate allocated block for this file.
   */
  BlockInfo getPenultimateBlock() {
    if (blocks == null || blocks.length <= 1) {
      return null;
    }
    return blocks[blocks.length - 2];
  }

  /**
   * Get the last block of the file.
   * Make sure it has the right type.
   */
  <T extends BlockInfo> T getLastBlock() throws IOException {
    if (blocks == null || blocks.length == 0)
      return null;
    T returnBlock = null;
    try {
      @SuppressWarnings("unchecked")  // ClassCastException is caught below
      T tBlock = (T)blocks[blocks.length - 1];
      returnBlock = tBlock;
    } catch(ClassCastException cce) {
      throw new IOException("Unexpected last block type: " 
          + blocks[blocks.length - 1].getClass().getSimpleName());
    }
    return returnBlock;
  }
  /**
   * Get the block of the file at index of i.
   * Make sure it has the right type.
   */
  <T extends BlockInfo> T getBlock(int i) throws IOException {
    if (blocks == null || blocks.length - 1 < i)
      return null;
    T returnBlock = null;
    try {
      @SuppressWarnings("unchecked")  // ClassCastException is caught below
      T tBlock = (T)blocks[i];
      returnBlock = tBlock;
    } catch(ClassCastException cce) {
      throw new IOException("Unexpected last block type: " 
          + blocks[i].getClass().getSimpleName());
    }
    return returnBlock;
  }

  /************* add by xianyu **************/
  BlockInfo getBlockByBlockID(long blockID){
	  for(int i = 0; i < numBlocks(); i++){
		  if(blocks[i].getBlockId() == blockID)
			  return blocks[i];
	  }
	  
	  return null;
  }
  /******************************************/
  
  int numBlocks() {
    return blocks == null ? 0 : blocks.length;
  }
}

