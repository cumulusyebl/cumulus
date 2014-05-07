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

import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.BlockUCState;
import org.apache.hadoop.hdfs.protocol.CodingMatrix;


class INodeFileUnderConstruction extends INodeFile {
  private  String clientName;         // lease holder
  private final String clientMachine;
  private final DatanodeDescriptor clientNode; // if client is a cluster node too.
  
 /**
   * modified by tony
   */
  INodeFileUnderConstruction(PermissionStatus permissions,
                             CodingMatrix matrix,
                             long fileSize,
                             long packetSize,
                             long modTime,
                             String clientName,
                             String clientMachine,
                             DatanodeDescriptor clientNode,HeaderBuffer headerbuf) {
    super(permissions.applyUMask(UMASK), matrix, modTime, modTime,
        fileSize, packetSize,headerbuf);
    this.clientName = clientName;
    this.clientMachine = clientMachine;
    this.clientNode = clientNode;
  }

  /**
   * modified by tony
   */
  public INodeFileUnderConstruction(byte[] name,
                             CodingMatrix matrix,
                             long fileSize,
                             long packetSize,
                             long modificationTime,
                             BlockInfo[] blocks,
                             PermissionStatus perm,
                             String clientName,
                             String clientMachine,
                             DatanodeDescriptor clientNode,HeaderBuffer headerbuf) {
    super(perm, matrix, blocks, modificationTime, modificationTime,
          fileSize, packetSize,headerbuf);
    setLocalName(name);
    this.clientName = clientName;
    this.clientMachine = clientMachine;
    this.clientNode = clientNode;
  }

  String getClientName() {
    return clientName;
  }

  void setClientName(String clientName) {
    this.clientName = clientName;
  }

  String getClientMachine() {
    return clientMachine;
  }

  DatanodeDescriptor getClientNode() {
    return clientNode;
  }

  /**
   * Is this inode being constructed?
   */
  @Override
  boolean isUnderConstruction() {
    return true;
  }

  //
  // converts a INodeFileUnderConstruction into a INodeFile
  // use the modification time as the access time
  //
  /**
   * modified by tony
   * @return
   */
  INodeFile convertToInodeFile() {
  /**INodeFile obj = new INodeFile(getPermissionStatus(),
                                  getMatrix(),
                                  getBlocks(),
                                  getModificationTime(),
                                  getModificationTime(),
                                  getFileSize(),
                                  getPacketSize(),getHeaderBuffer());
   **/
	  
    INodeFile obj = new INodeFile(this);
    return obj;
    
  }

  /**
   * Remove a block from the block list. This block should be
   * the last one on the list.
   */
  void removeLastBlock(Block oldblock) throws IOException {
    if (blocks == null) {
      throw new IOException("Trying to delete non-existant block " + oldblock);
    }
    int size_1 = blocks.length - 1;
    if (!blocks[size_1].equals(oldblock)) {
      throw new IOException("Trying to delete non-last block " + oldblock);
    }

    //copy to a new list
    BlockInfo[] newlist = new BlockInfo[size_1];
    System.arraycopy(blocks, 0, newlist, 0, size_1);
    blocks = newlist;
  }

  /**
   * Convert the last block of the file to an under-construction block.
   * Set its locations.
   */
  BlockInfoUnderConstruction setLastBlock(BlockInfo lastBlock,
                                          DatanodeDescriptor[] targets)
  throws IOException {
    if (blocks == null || blocks.length == 0) {
      throw new IOException("Trying to update non-existant block. " +
          "File is empty.");
    }
    BlockInfoUnderConstruction ucBlock =
      lastBlock.convertToBlockUnderConstruction(
          BlockUCState.UNDER_CONSTRUCTION, targets);
    ucBlock.setINode(this);
    setBlock(numBlocks()-1, ucBlock);
    return ucBlock;
  }
}
