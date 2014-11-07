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

import static org.apache.hadoop.hdfs.protocol.DataTransferProtocol.Status.ERROR_ACCESS_TOKEN;
import static org.apache.hadoop.hdfs.protocol.DataTransferProtocol.Status.SUCCESS;

import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.InterruptedIOException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.EnumSet;
import java.util.LinkedList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSOutputSummer;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Syncable;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DSQuotaExceededException;
import org.apache.hadoop.hdfs.protocol.DataTransferProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.NSQuotaExceededException;
import org.apache.hadoop.hdfs.protocol.RegeneratingCodeMatrix;
import org.apache.hadoop.hdfs.protocol.UnresolvedPathException;
import org.apache.hadoop.hdfs.protocol.DataTransferProtocol.BlockConstructionStage;
import org.apache.hadoop.hdfs.protocol.DataTransferProtocol.PacketHeader;
import org.apache.hadoop.hdfs.protocol.DataTransferProtocol.PipelineAck;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.protocol.CodingMatrix;
import org.apache.hadoop.hdfs.server.namenode.NotReplicatedYetException;
import org.apache.hadoop.hdfs.server.namenode.SafeModeException;
import org.apache.hadoop.io.EnumSetWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.PureJavaCrc32;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.hdfs.security.token.block.InvalidBlockTokenException;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.protocol.RSCoderProtocol;
import org.hamcrest.core.IsInstanceOf;

import sun.util.logging.resources.logging;

/****************************************************************
 * DFSOutputStream creates files from a stream of bytes.
 * 
 * The client application writes data that is cached internally by this stream.
 * Data is broken up into packets, each packet is typically 64K in size. A
 * packet comprises of chunks. Each chunk is typically 512 bytes and has an
 * associated checksum with it.
 * 
 * When a client application fills up the currentPacket, it is enqueued into
 * dataQueue. The DataStreamer thread picks up packets from the dataQueue, sends
 * it to the first datanode in the pipeline and moves it from the dataQueue to
 * the ackQueue. The ResponseProcessor receives acks from the datanodes. When an
 * successful ack for a packet is received from all datanodes, the
 * ResponseProcessor removes the corresponding packet from the ackQueue.
 * 
 * In case of error, all outstanding packets and moved from ackQueue. A new
 * pipeline is setup by eliminating the bad datanode from the original pipeline.
 * The DataStreamer now starts sending packets from the dataQueue.
 ****************************************************************/
class DFSOutputStream extends FSOutputSummer implements Syncable {
  /**
   * 
   */
  private final DFSClient dfsClient;
  private Configuration conf;
  private static final int MAX_PACKETS = 80; // each packet 64K, total 5MB
  private Socket[] s = null;
  // closed is accessed by different threads under different locks.
  private volatile boolean closed = false;
  private String src;
  private Block[] blocks;
  private long fileSize;
  private final DataChecksum checksum;
  // both dataQueue and ackQueue are protected by dataQueue lock
  // private final LinkedList<Packet> dataQueue = new LinkedList<Packet>();
  private ArrayList<LinkedList<Packet>> dataQueue = null;
  // private final LinkedList<Packet> ackQueue = new LinkedList<Packet>();
  private ArrayList<LinkedList<Packet>> ackQueue = null;
  private Packet currentPacket = null;
  private DataStreamer streamer;
  private long currentSeqno = 0;
  private long lastQueuedSeqno = -1;
  private long lastAckedSeqno = -1;
  private long bytesCurBlock = 0; // bytes writen in current block
  private int packetSize = 0; // write packet size, including the header.
  private int chunksPerPacket = 0;
  private volatile IOException lastException = null;
  private long artificialSlowdown = 0;
  private long lastFlushOffset = 0; // offset when flush was invoked
  // persist blocks on namenode
  private final AtomicBoolean persistBlocks = new AtomicBoolean(false);
  private volatile boolean appendChunk = false; // appending to existing partial
                                                // block
  private long initialFileSize = 0; // at time of file open
  private Progressable progress;
  private short blockReplication; // replication factor of file
  private CodingMatrix matrix;
  private Packet[] codeBuf;
  int offsetInB = 0;
  private boolean[] queueEnd;
  private Object object = new Object();
  private Object waitCheckSum = new Object();
  private int count = 0;
  private int chckSumCount = 0;
  private ExecutorService pool;

  // 1-8 add by tianshan
  /** wheather the datanode is available */
  private boolean dnState[];
  /** the index of error datanode */
  private int dfsErrorIndex = -1;
  /** the error datanode's reference */
  private DatanodeInfo errNode = null;

  private class Packet {
    long seqno; // sequencenumber of buffer in block
    long offsetInBlock; // offset in block
    boolean lastPacketInBlock; // is this the last packet in block?
    int numChunks; // number of chunks currently in packet
    int maxChunks; // max chunks in packet

    /** buffer for accumulating packet checksum and data */
    ByteBuffer buffer; // wraps buf, only one of these two may be non-null
    byte[] buf;

    /**
     * buf is pointed into like follows: (C is checksum data, D is payload data)
     * 
     * [HHHHHCCCCC________________DDDDDDDDDDDDDDDD___] ^ ^ ^ ^ | checksumPos
     * dataStart dataPos checksumStart
     */
    int checksumStart;
    int dataStart;
    int dataPos;
    int checksumPos;

    private static final long HEART_BEAT_SEQNO = -1L;

    /**
     * create a heartbeat packet
     */
    Packet() {
      this.lastPacketInBlock = false;
      this.numChunks = 0;
      this.offsetInBlock = 0;
      this.seqno = HEART_BEAT_SEQNO;

      buffer = null;
      int packetSize = PacketHeader.PKT_HEADER_LEN + DFSClient.SIZE_OF_INTEGER; // TODO(todd)
                                                                                // strange
      buf = new byte[packetSize];

      checksumStart = dataStart = packetSize;
      checksumPos = checksumStart;
      dataPos = dataStart;
      maxChunks = 0;
    }

    Packet(Packet packet) {
      this.lastPacketInBlock = packet.lastPacketInBlock;
      this.numChunks = packet.numChunks;
      this.offsetInBlock = packet.offsetInBlock;
      this.seqno = packet.seqno;

      buffer = null;
      int packetSize = packet.buf.length;
      buf = new byte[packetSize];
      dataStart = packet.dataStart;
      dataPos = packet.dataPos;
      checksumStart = packet.checksumStart;
      checksumPos = packet.checksumPos;

      System.arraycopy(packet.buf, 0, buf, 0, packetSize);

    }

    // create a new packet
    Packet(int pktSize, int chunksPerPkt, long offsetInBlock) {
      this.lastPacketInBlock = false;
      this.numChunks = 0;
      this.offsetInBlock = offsetInBlock;
      this.seqno = currentSeqno;
      currentSeqno++;

      buffer = null;
      buf = new byte[pktSize];

      checksumStart = PacketHeader.PKT_HEADER_LEN;
      checksumPos = checksumStart;
      dataStart = checksumStart + chunksPerPkt * checksum.getChecksumSize();
      dataPos = dataStart;
      maxChunks = chunksPerPkt;
    }

    public void setOffset(long offset) {
      offsetInBlock = offset;
    }

    void writeData(byte[] inarray, int off, int len) {
      if (dataPos + len > buf.length) {
        throw new BufferOverflowException();
      }
      System.arraycopy(inarray, off, buf, dataPos, len);
      dataPos += len;
    }

    void writeChecksum(byte[] inarray, int off, int len) {
      if (checksumPos + len > dataStart) {
        throw new BufferOverflowException();
      }
      System.arraycopy(inarray, off, buf, checksumPos, len);
      checksumPos += len;
    }

    /**
     * Returns ByteBuffer that contains one full packet, including header.
     */
    ByteBuffer getBuffer() {
      /*
       * Once this is called, no more data can be added to the packet. setting
       * 'buf' to null ensures that. This is called only when the packet is
       * ready to be sent.
       */
      if (buffer != null) {
        return buffer;
      }

      // prepare the header and close any gap between checksum and data.

      int dataLen = dataPos - dataStart;
      int checksumLen = checksumPos - checksumStart;

      if (checksumPos != dataStart) {
        /*
         * move the checksum to cover the gap. This can happen for the last
         * packet.
         */
        System.arraycopy(buf, checksumStart, buf, dataStart - checksumLen,
            checksumLen);
      }

      int pktLen = DFSClient.SIZE_OF_INTEGER + dataLen + checksumLen;

      // normally dataStart == checksumPos, i.e., offset is zero.
      buffer = ByteBuffer.wrap(buf, dataStart - checksumPos,
          PacketHeader.PKT_HEADER_LEN + pktLen - DFSClient.SIZE_OF_INTEGER);
      buf = null;
      buffer.mark();

      PacketHeader header = new PacketHeader(pktLen, offsetInBlock, seqno,
          lastPacketInBlock, dataLen);
      header.putInBuffer(buffer);

      buffer.reset();
      return buffer;
    }

    // codeBuf[i].mult(matrix.getElemAt(row, i));
    void mult(byte element) {
    	
    	int len = dataPos - dataStart;
    	// seq LCTBLK.4 1
		// modified by ds at 2014-5-7
		// modified modified by ds begins
		// //for (int i = 0; i < len; i++)
		// //{
		// // this.buf[this.dataStart + i] = matrix.mult(this.buf[this.dataStart + i], element);
		// //}
		if (element == 0)
		{
			for (int i = 0; i < len; i++)
			{
				this.buf[this.dataStart + i] = (byte) 0;
			}
		}
		else
		{
			for (int i = 0; i < len; i++)
			{
				this.buf[this.dataStart + i] = matrix.mult(this.buf[this.dataStart + i], element);
			}
		}
		// modified by ds ends
    }

    /**
     * @author zdy
     * @param p
     *          ,element
     * @return precondition: this.buf.lenght >= p.buf.lenght.
     */

    void code(Packet p, byte element) {

      // long start = System.currentTimeMillis();

      int len = p.dataPos - p.dataStart;
	    
      	// seq LCTBLK.4 2
		// added by ds at 2014-5-7
		if (element == 0)
		{// this case packet unchanged
			return;
		}
		for (int i = 0; i < len; i++)
		{
			this.buf[this.dataStart + i] = matrix.code(this.buf[this.dataStart + i], p.buf[p.dataStart + i],
					element);
		}

      // int flag = dataStart;
      // int checksumPerPacket = (checksumPos - checksumStart)/4;
      // checksumPos = checksumStart;
      // DataChecksum checksum =
      // DataChecksum.newDataChecksum(DataChecksum.CHECKSUM_CRC32,512);
      // for(int i = 0;i < checksumPerPacket;i++){
      // checksum.reset();
      // if(dataPos - flag < 512)
      // checksum.update(buf,flag,dataPos-flag);
      // else{
      // checksum.update(buf,flag,512);
      // flag += 512;
      // }
      // byte[] bytes = new byte[4];
      // int integer = (int) checksum.getValue();
      // bytes[0] = (byte)((integer >>> 24) & 0xFF);
      // bytes[1] = (byte)((integer >>> 16) & 0xFF);
      // bytes[2] = (byte)((integer >>> 8) & 0xFF);
      // bytes[3] = (byte)((integer >>> 0) & 0xFF);
      // System.arraycopy(bytes,0,buf,checksumPos,4);
      // checksumPos += 4;
      // }

      // DFSClient.LOG.info("code time: "+(System.currentTimeMillis() - start));
    }

    /*
     * 
     * @author vither
     * 
     * @param p
     * 
     * @return precondition: this.buf.lenght >= p.buf.lenght.
     * 
     * 
     * 
     * void code(Packet p,byte element) {
     * 
     * long start = System.currentTimeMillis();
     * 
     * short[] InputBytes; short[] Buf; short key; int len = p.dataPos -
     * p.dataStart; InputBytes= new short[len]; Buf= new short[len]; for (int i
     * = 0; i < len; i++){ if (p.buf[p.dataStart + i]< 0) InputBytes[i]
     * =(short)(p.buf[p.dataStart + i]+256); else
     * InputBytes[i]=(short)p.buf[p.dataStart + i];
     * 
     * if (this.buf[this.dataStart + i] < 0) Buf[i]
     * =(short)(this.buf[this.dataStart + i]+256); else
     * Buf[i]=(short)this.buf[this.dataStart + i];
     * 
     * if(element < 0) key= (short)(element + 256); else key=(short)(element);
     * Buf[i] ^= RSCoderProtocol.mult[InputBytes[i]][key]; } for (int i = 0; i <
     * len; ++i) { this.buf[this.dataStart + i]=(byte)Buf[i]; }
     * 
     * // DFSClient.LOG.info("before update checksumPos: "+checksumPos); //
     * byte[] bytes; // int flag = dataStart; // int checksumPerPacket =
     * (checksumPos - checksumStart)/4; // checksumPos = checksumStart; //
     * DataChecksum checksum =
     * DataChecksum.newDataChecksum(DataChecksum.CHECKSUM_CRC32, // 512); //
     * DFSClient.LOG.info("chunksPerPacket: "+chunksPerPacket); // for (int i =
     * 0;i < checksumPerPacket;i++){ // // checksum.reset(); //
     * if(dataPos-flag<512) // checksum.update(buf,flag,dataPos-flag); // else {
     * // checksum.update(buf,flag,512); // flag += 512; // } // bytes = new
     * byte[4]; // int integer = (int) checksum.getValue(); // bytes[0] =
     * (byte)((integer >>> 24) & 0xFF); // bytes[1] = (byte)((integer >>> 16) &
     * 0xFF); // bytes[2] = (byte)((integer >>> 8) & 0xFF); // bytes[3] =
     * (byte)((integer >>> 0) & 0xFF); // System.arraycopy(bytes, 0, buf,
     * checksumPos, 4); // checksumPos += 4; // } // //
     * //DFSClient.LOG.info("code time: "+(System.currentTimeMillis()-start));
     * // DFSClient.LOG.info("after update checksumPos: "+checksumPos); }
     */

    void generateCheckSum() {
      // DFSClient.LOG.info("before update checksumPos: "+checksumPos);
      byte[] bytes;
      int flag = dataStart;
      int checksumPerPacket = (dataPos - dataStart - 1) / 512 + 1;
      checksumPos = checksumStart;
      DataChecksum checksum = DataChecksum.newDataChecksum(
          DataChecksum.CHECKSUM_CRC32, 512);
      for (int i = 0; i < checksumPerPacket; i++) {

        checksum.reset();
        if (dataPos - flag < 512)
          checksum.update(buf, flag, dataPos - flag);
        else {
          checksum.update(buf, flag, 512);
          flag += 512;
        }
        bytes = new byte[4];
        int integer = (int) checksum.getValue();
        bytes[0] = (byte) ((integer >>> 24) & 0xFF);
        bytes[1] = (byte) ((integer >>> 16) & 0xFF);
        bytes[2] = (byte) ((integer >>> 8) & 0xFF);
        bytes[3] = (byte) ((integer >>> 0) & 0xFF);
        System.arraycopy(bytes, 0, buf, checksumPos, 4);
        checksumPos += 4;
      }

      // DFSClient.LOG.info("code time: "+(System.currentTimeMillis()-start));
      // DFSClient.LOG.info("after update checksumPos: "+checksumPos);
    }

    // get the packet's last byte's offset in the block
    long getLastByteOffsetBlock() {
      return offsetInBlock + dataPos - dataStart;
    }

    /**
     * Check if this packet is a heart beat packet
     * 
     * @return true if the sequence number is HEART_BEAT_SEQNO
     */
    private boolean isHeartbeatPacket() {
      return seqno == HEART_BEAT_SEQNO;
    }

    public String toString() {
      return "packet seqno:" + this.seqno + " offsetInBlock:"
          + this.offsetInBlock + " lastPacketInBlock:" + this.lastPacketInBlock
          + " lastByteOffsetInBlock: " + this.getLastByteOffsetBlock();
    }

  }

  //
  // The DataStreamer class is responsible for sending data packets to the
  // datanodes in the pipeline. It retrieves a new blockid and block locations
  // from the namenode, and starts streaming packets to the pipeline of
  // Datanodes. Every packet has a sequence number associated with
  // it. When all the packets for a block are sent out and acks for each
  // if them are received, the DataStreamer closes the current block.
  //
  class DataStreamer extends Daemon {
    private volatile boolean streamerClosed = false;

    private Token<BlockTokenIdentifier> accessToken;
    private DataOutputStream[] blockStream;
    private DataInputStream[] blockReplyStream;
    private ResponseProcessor response = null;
    private volatile DatanodeInfo[] nodes = null; // list of targets for current
                                                  // file
    private ArrayList<DatanodeInfo> excludedNodes = new ArrayList<DatanodeInfo>();
    volatile boolean hasError = false;
    volatile int errorIndex = -1;
    private BlockConstructionStage stage; // block construction stage
    private long bytesSent = 0; // number of bytes that've been sent

    // int num;

    /**
     * Default construction for file create
     */
    private DataStreamer() {
      stage = BlockConstructionStage.PIPELINE_SETUP_CREATE;
      int n = matrix.getColumn();

      blockStream = new DataOutputStream[n];
      // num = n;
      blockReplyStream = new DataInputStream[n];
    }

    /**
     * TODO: Not supported recently. Construct a data streamer for append
     * 
     * @param lastBlock
     *          last block of the file to be appended
     * @param stat
     *          status of the file to be appended
     * @param bytesPerChecksum
     *          number of bytes per checksum
     * @throws IOException
     *           if error occurs
     */
    private DataStreamer(LocatedBlock lastBlock, HdfsFileStatus stat,
        int bytesPerChecksum) throws IOException {
      // stage = BlockConstructionStage.PIPELINE_SETUP_APPEND;
      // block = lastBlock.getBlock();
      // bytesSent = block.getNumBytes();
      // accessToken = lastBlock.getBlockToken();
      // long usedInLastBlock = stat.getLen() % blockSize;
      // int freeInLastBlock = (int)(blockSize - usedInLastBlock);
      //
      // // calculate the amount of free space in the pre-existing
      // // last crc chunk
      // int usedInCksum = (int)(stat.getLen() % bytesPerChecksum);
      // int freeInCksum = bytesPerChecksum - usedInCksum;
      //
      // // if there is space in the last block, then we have to
      // // append to that block
      // if (freeInLastBlock == blockSize) {
      // throw new IOException("The last block for file " +
      // src + " is full.");
      // }
      //
      // if (usedInCksum > 0 && freeInCksum > 0) {
      // // if there is space in the last partial chunk, then
      // // setup in such a way that the next packet will have only
      // // one chunk that fills up the partial chunk.
      // //
      // computePacketChunkSize(0, freeInCksum);
      // resetChecksumChunk(freeInCksum);
      // appendChunk = true;
      // } else {
      // // if the remaining space in the block is smaller than
      // // that expected size of of a packet, then create
      // // smaller size packet.
      // //
      // computePacketChunkSize(Math.min(dfsClient.writePacketSize,
      // freeInLastBlock),
      // bytesPerChecksum);
      // }
      //
      // // setup pipeline to append to the last block XXX retries??
      // nodes = lastBlock.getLocations();
      // errorIndex = -1; // no errors yet.
      // if (nodes.length < 1) {
      // throw new IOException("Unable to retrieve blocks locations " +
      // " for last block " + block +
      // "of file " + src);
      //
      // }
    }

    /**
     * Initialize for data streaming
     */
    private void initDataStreaming() {
      this.setName("DataStreamer for file " + src);
      response = new ResponseProcessor(nodes);
      response.start();
      stage = BlockConstructionStage.DATA_STREAMING;

    }

    // TODO:
    /*
     * Maybe we don't need this function.
     */
    private void endBlock() {
      // if(DFSClient.LOG.isDebugEnabled()) {
      // DFSClient.LOG.debug("Closing old block " + block);
      // }
      this.setName("DataStreamer for file " + src);
      closeResponder();
      closeStream();
      nodes = null;
      stage = BlockConstructionStage.PIPELINE_SETUP_CREATE;
    }

    public void run() {
      long lastPacket = System.currentTimeMillis();
      int num = blockStream.length;

      while (!streamerClosed && dfsClient.clientRunning) {

        // 1-8 change by tianshan,
        // if the Responder encountered an error,
        // just shutdown the error one
        
        // if the Responder encountered an error, shutdown Responder
//        if (hasError && response != null) {
//          try {
//            response.close();
//            response.join();
//            response = null;
//          } catch (InterruptedException e) {
//          }
//        }

        Packet one = null;

        try {
          // process datanode IO errors if any
          boolean doSleep = false;
          if (hasError && errorIndex >= 0) {
            doSleep = processDatanodeError();
          }
          // get new block from namenode.
          if (stage == BlockConstructionStage.PIPELINE_SETUP_CREATE) {
            if (DFSClient.LOG.isDebugEnabled()) {
              DFSClient.LOG.debug("Allocating new block");
            }
            nodes = nextBlockOutputStream(src);
            initDataStreaming();
          } else if (stage == BlockConstructionStage.PIPELINE_SETUP_APPEND) {
            // TODO: not supported recently.
            // if(DFSClient.LOG.isDebugEnabled()) {
            // DFSClient.LOG.debug("Append to block " + block);
            // }
            // setupPipelineForAppendOrRecovery();
            // initDataStreaming();
          }
          // get packet to be sent.
          for (int i = 0; i < dataQueue.size(); ++i) {
            // 1-8 add by tianshan
            if (dnState[i] == false) {
              synchronized(dataQueue.get(i)) {
                if ( !dataQueue.get(i).isEmpty() ) {
                  Packet onex = dataQueue.get(i).getFirst();
                  // if the packet is  null, set the block size 
                  // otherwise, the namenode get recovery block will verify false
                  if (onex.lastPacketInBlock == true) {
                    blocks[i].setNumBytes(onex.getLastByteOffsetBlock());
                  }
                  dataQueue.get(i).removeFirst();
                }
              }
              continue;
            }
            
            
            synchronized (dataQueue.get(i)) {
              // wait for a packet to be sent.
              long now = System.currentTimeMillis();
              while ((!streamerClosed && !hasError && dfsClient.clientRunning
                  && dataQueue.get(i).isEmpty() && !queueEnd[i] && (stage != BlockConstructionStage.DATA_STREAMING || stage == BlockConstructionStage.DATA_STREAMING
                  && now - lastPacket < dfsClient.socketTimeout / 2))
                  || doSleep) {
                long timeout = dfsClient.socketTimeout / 2 - (now - lastPacket);
                timeout = timeout <= 0 ? 1000 : timeout;
                timeout = (stage == BlockConstructionStage.DATA_STREAMING) ? timeout
                    : 1000;
                try {
                  // DFSClient.LOG.info("timeout: " + timeout);
                  dataQueue.get(i).wait(timeout);
                } catch (InterruptedException e) {
                }
                doSleep = false;
                now = System.currentTimeMillis();
              }
              if (streamerClosed || hasError || !dfsClient.clientRunning) {
                continue;
              }
              if (dataQueue.get(i).isEmpty()) {
                continue;
                // one = new Packet(); // heartbeat packet
              } else {

                one = dataQueue.get(i).getFirst(); // regular data packet
                // DFSClient.LOG.info("Getting packet from dataQueue at index:"
                // + i
                // + "offset=" + one.offsetInBlock);
              }
              assert one != null;

              // long lastByteOffsetInBlock = one.getLastByteOffsetBlock();
              // if (lastByteOffsetInBlock > blockSize) {
              // throw new IOException("BlockSize " + blockSize +
              // " is smaller than data size. " +
              // " Offset of packet in block " +
              // lastByteOffsetInBlock +
              // " Aborting file " + src);
              // }

              // if (one.lastPacketInBlock) {
              // // wait for all data packets have been successfully acked
              // synchronized (dataQueue.get(i)) {
              // while (!streamerClosed && !hasError &&
              // ackQueue.get(i).size() != 0 && dfsClient.clientRunning) {
              // try {
              // // wait for acks to arrive from datanodes
              // dataQueue.get(i).wait(1000);
              // } catch (InterruptedException e) {
              // }
              // }
              // }
              // if (streamerClosed || hasError || !dfsClient.clientRunning) {
              // continue;
              // }
              // if (--num == 0) {
              // stage = BlockConstructionStage.PIPELINE_CLOSE;
              // }
              //
              // }

              // send the packet
              ByteBuffer buf = one.getBuffer();

              if (!one.isHeartbeatPacket()) {
                synchronized (dataQueue.get(i)) {
                  // move packet from dataQueue to ackQueue
                  dataQueue.get(i).removeFirst();
                  // ackQueue.get(i).addLast(one);
                  dataQueue.get(i).notifyAll();
                }
                synchronized (ackQueue.get(i)) {
                  ackQueue.get(i).addLast(one);
                }
              }

              if (DFSClient.LOG.isDebugEnabled()) {
                DFSClient.LOG.debug("DataStreamer block " + blocks[i]
                    + " sending packet " + one);
              }

              // write out data to remote datanode
              blockStream[i]
                  .write(buf.array(), buf.position(), buf.remaining());
              blockStream[i].flush();
              lastPacket = System.currentTimeMillis();
              if (one.isHeartbeatPacket()) { // heartbeat packet
              }
              // update bytesSent
              long tmpBytesSent = one.getLastByteOffsetBlock();
              if (bytesSent < tmpBytesSent) {
                bytesSent = tmpBytesSent;
              }
              if (streamerClosed || hasError || !dfsClient.clientRunning) {
                continue;
              }
              if (one.lastPacketInBlock) {
                // wait for the close packet has been acked
                --num;
                queueEnd[i] = true;
                if (num == 0) {
                  synchronized (dataQueue.get(i)) {
                    int ackSize = 0;
                    for (LinkedList<Packet> aq : ackQueue) {
                      ackSize += aq.size();
                    }
                    while (!streamerClosed && !hasError && ackSize != 0
                        && dfsClient.clientRunning) {
                      dataQueue.get(i).wait(1000);// wait for acks to arrive
                                                  // from datanodes
                    }
                  }
                  if (streamerClosed || hasError || !dfsClient.clientRunning) {
                    continue;
                  }
                  endBlock();
                  stage = BlockConstructionStage.PIPELINE_CLOSE;
                }
              }
              if (progress != null) {
                progress.progress();
              }

              // This is used by unit test to trigger race conditions.
              if (artificialSlowdown != 0 && dfsClient.clientRunning) {
                Thread.sleep(artificialSlowdown);
              }
            }

          }

        } catch (Throwable e) {
          DFSClient.LOG.warn("DataStreamer Exception: "
              + StringUtils.stringifyException(e));
          if (e instanceof IOException) {
            setLastException((IOException) e);
          }
          hasError = true;
          if (errorIndex == -1) { // not a datanode error
            streamerClosed = true;
          }
        }
      }
      closeInternal();
    }

    /*
     * streamer thread is the only thread that opens streams to datanode, and
     * closes them. Any error recovery is also done by this thread.
     */
    // public void run() {
    // //long lastPacket = System.currentTimeMillis();
    //
    //
    //
    // try {
    // nodes = nextBlockOutputStream(src);
    // } catch (IOException e1) {
    // // TODO Auto-generated catch block
    // DFSClient.LOG.info(e1.toString());
    // }
    // initDataStreaming();
    // //DFSClient.LOG.info("length: "+blockStream.length);
    //
    // for (int i = 0; i < blockStream.length; i++) {
    // new Thread(new miniSend(i)).start();
    // }
    // while (!streamerClosed && dfsClient.clientRunning) {
    // synchronized (lockObject) {
    // for (int i = 0; i < blockStream.length; i++) {
    // dataQueue.get(i).notify();
    // }
    // try {
    // lockObject.wait();
    // lockObject.notify();
    // } catch (InterruptedException e) {
    // // TODO Auto-generated catch block
    // e.printStackTrace();
    // }
    //
    //
    // }
    //
    // // if the Responder encountered an error, shutdown Responder
    // if (hasError && response != null) {
    // try {
    // response.close();
    // response.join();
    // response = null;
    // } catch (InterruptedException e) {
    // }
    // }
    //
    // Packet one = null;
    //
    // try {
    // // process datanode IO errors if any
    // boolean doSleep = false;
    // if (hasError && errorIndex>=0) {
    // doSleep = processDatanodeError();
    // }
    // // get new block from namenode.
    // if (stage == BlockConstructionStage.PIPELINE_SETUP_CREATE) {
    // if(DFSClient.LOG.isDebugEnabled()) {
    // DFSClient.LOG.debug("Allocating new block");
    // }
    // nodes = nextBlockOutputStream(src);
    // initDataStreaming();
    // } else if (stage == BlockConstructionStage.PIPELINE_SETUP_APPEND) {
    // // TODO: not supported recently.
    // // if(DFSClient.LOG.isDebugEnabled()) {
    // // DFSClient.LOG.debug("Append to block " + block);
    // // }
    // // setupPipelineForAppendOrRecovery();
    // // initDataStreaming();
    // }
    // // get packet to be sent.
    // for (int i = 0; i < dataQueue.size(); ++i) {
    // synchronized (dataQueue.get(i)) {
    // // wait for a packet to be sent.
    // long now = System.currentTimeMillis();
    // while ((!streamerClosed && !hasError && dfsClient.clientRunning
    // && dataQueue.get(i).isEmpty() && !queueEnd[i] &&
    // (stage != BlockConstructionStage.DATA_STREAMING ||
    // stage == BlockConstructionStage.DATA_STREAMING &&
    // now - lastPacket < dfsClient.socketTimeout/2)) || doSleep ) {
    // long timeout = dfsClient.socketTimeout/2 - (now-lastPacket);
    // timeout = timeout <= 0 ? 1000 : timeout;
    // timeout = (stage == BlockConstructionStage.DATA_STREAMING)?
    // timeout : 1000;
    // try {
    // //DFSClient.LOG.info("timeout: " + timeout);
    // dataQueue.get(i).wait(timeout);
    // } catch (InterruptedException e) {
    // }
    // doSleep = false;
    // now = System.currentTimeMillis();
    // }
    // if (streamerClosed || hasError || !dfsClient.clientRunning) {
    // continue;
    // }
    // if (dataQueue.get(i).isEmpty()) {
    // continue;
    // //one = new Packet(); // heartbeat packet
    // } else {
    //
    // one = dataQueue.get(i).getFirst(); // regular data packet
    // //DFSClient.LOG.info("Getting packet from dataQueue at index:" + i
    // //+ "offset=" + one.offsetInBlock);
    // }
    // assert one != null;
    //
    // // long lastByteOffsetInBlock = one.getLastByteOffsetBlock();
    // // if (lastByteOffsetInBlock > blockSize) {
    // // throw new IOException("BlockSize " + blockSize +
    // // " is smaller than data size. " +
    // // " Offset of packet in block " +
    // // lastByteOffsetInBlock +
    // // " Aborting file " + src);
    // // }
    //
    // // if (one.lastPacketInBlock) {
    // // // wait for all data packets have been successfully acked
    // // synchronized (dataQueue.get(i)) {
    // // while (!streamerClosed && !hasError &&
    // // ackQueue.get(i).size() != 0 && dfsClient.clientRunning) {
    // // try {
    // // // wait for acks to arrive from datanodes
    // // dataQueue.get(i).wait(1000);
    // // } catch (InterruptedException e) {
    // // }
    // // }
    // // }
    // // if (streamerClosed || hasError || !dfsClient.clientRunning) {
    // // continue;
    // // }
    // // if (--num == 0) {
    // // stage = BlockConstructionStage.PIPELINE_CLOSE;
    // // }
    // //
    // // }
    //
    // // send the packet
    // ByteBuffer buf = one.getBuffer();
    //
    // if (!one.isHeartbeatPacket()) {
    // synchronized (dataQueue.get(i)) {
    // // move packet from dataQueue to ackQueue
    // dataQueue.get(i).removeFirst();
    // ackQueue.get(i).addLast(one);
    // dataQueue.get(i).notifyAll();
    // }
    // }
    //
    // if (DFSClient.LOG.isDebugEnabled()) {
    // DFSClient.LOG.debug("DataStreamer block " + blocks[i] +
    // " sending packet " + one);
    // }
    //
    // // write out data to remote datanode
    // blockStream[i].write(buf.array(), buf.position(), buf.remaining());
    // blockStream[i].flush();
    // lastPacket = System.currentTimeMillis();
    // if (one.isHeartbeatPacket()) { //heartbeat packet
    // }
    // // update bytesSent
    // long tmpBytesSent = one.getLastByteOffsetBlock();
    // if (bytesSent < tmpBytesSent) {
    // bytesSent = tmpBytesSent;
    // }
    // if (streamerClosed || hasError || !dfsClient.clientRunning) {
    // continue;
    // }
    // if (one.lastPacketInBlock) {
    // // wait for the close packet has been acked
    // --num;
    // queueEnd[i] = true;
    // if(num == 0){
    // synchronized (dataQueue.get(i)) {
    // int ackSize = 0;
    // for (LinkedList<Packet> aq : ackQueue) {
    // ackSize += aq.size();
    // }
    // while (!streamerClosed && !hasError &&
    // ackSize != 0 && dfsClient.clientRunning) {
    // dataQueue.get(i).wait(1000);// wait for acks to arrive from datanodes
    // }
    // }
    // if (streamerClosed || hasError || !dfsClient.clientRunning) {
    // continue;
    // }
    // endBlock();
    // stage = BlockConstructionStage.PIPELINE_CLOSE;
    // }
    // }
    // if (progress != null) { progress.progress(); }
    //
    // // This is used by unit test to trigger race conditions.
    // if (artificialSlowdown != 0 && dfsClient.clientRunning) {
    // Thread.sleep(artificialSlowdown);
    // }
    // }
    //
    // }
    //
    //
    //
    // } catch (Throwable e) {
    // DFSClient.LOG.warn("DataStreamer Exception: " +
    // StringUtils.stringifyException(e));
    // if (e instanceof IOException) {
    // setLastException((IOException)e);
    // }
    // hasError = true;
    // if (errorIndex == -1) { // not a datanode error
    // streamerClosed = true;
    // }
    // }
    // }
    // closeInternal();
    // }

    private void closeInternal() {
      closeResponder(); // close and join
      closeStream();
      streamerClosed = true;
      closed = true;
      synchronized (dataQueue) {
        dataQueue.notifyAll();
      }
    }

    /*
     * close both streamer and DFSOutputStream, should be called only by an
     * external thread and only after all data to be sent has been flushed to
     * datanode.
     * 
     * Interrupt this data streamer if force is true
     * 
     * @param force if this data stream is forced to be closed
     */
    void close(boolean force) {
      streamerClosed = true;
      synchronized (dataQueue) {
        dataQueue.notifyAll();
      }//  /**
 //   * using Xor Decoder
 //   * revised by zdy @ 2013/10/28
 //   */
 // private int XORDecoder(short[][] g,byte[][] Buf,int[] buflen,int offset,byte[] buf) throws IOException{
//     //g是编码矩阵，Buf是用于解码的数据，buflen对应Buf里数据的长度，解码得到的数据放到buf中从offset开始的位置
//     //返回的是buf中写入的长度
//     int len = 0;
//     int off = offset;
//     g = rsp.InitialInvertedCauchyMatrix(g);
 //
//     for(int t = 0; t < k;t++){
//       len = 0;
//       for(int j = 0;j < k;j++){
//         if(g[j][t] != 0){
//           if(buflen[j] != -1 && len < buflen[j])
//             len = buflen[j];
//         }
//       }
//       short[] Output = new short[len];
//       Arrays.fill(Output,(short)0);
//       for(int i=0;i < k;i++){
//         if(g[i][t] != 0){
//           for(int j = 0;j < buflen[i];j++)
//             Output[j] = (short) (Output[j] ^ Buf[i][j]);
//         }
//       }
 //
//       for(int i = 0 ;i < Output.length;i++)
//         buf[off++] = (byte)Output[i];
//     }
 //
//     return (off - offset);
 //  }
      if (force) {
        this.interrupt();
      }
    }

    private void closeResponder() {
      if (response != null) {
        try {
          response.close();
          response.join();
        } catch (InterruptedException e) {
        } finally {
          response = null;
        }
      }
    }

    private void closeStream() {
      if (blockStream != null) {
        for (DataOutputStream bs : blockStream) {
          try {
            bs.close();
          } catch (IOException e) {
          } finally {
            blockStream = null;
          }
        }

      }
      if (blockReplyStream != null) {
        for (DataInputStream rs : blockReplyStream) {
          try {
            rs.close();
          } catch (IOException e) {
          } finally {
            blockReplyStream = null;
          }
        }

      }
    }

    //
    // Processes reponses from the datanodes. A packet is removed
    // from the ackQueue when its response arrives.
    //
    private class ResponseProcessor extends Daemon {

      private volatile boolean responderClosed = false;
      private DatanodeInfo[] targets = null;
      private boolean isLastPacketInBlock = false;

      ResponseProcessor(DatanodeInfo[] targets) {
        this.targets = targets;
      }

      public void run() {
        setName("ResponseProcessor for file " + src);
        PipelineAck ack = new PipelineAck();
        while (!responderClosed && dfsClient.clientRunning
            && !isLastPacketInBlock) {
          for (int i = 0; i < blockReplyStream.length; i++) {
            // 1-8 add by tianshan
            // if dn is not available,just skip
            if (dnState[i] == false)
              continue;
            
            isLastPacketInBlock = false;

            // process responses from datanodes.
            try {
              // read an ack from the pipeline
              // DFSClient.LOG.info("in while i: "+i);

              ack.readFields(blockReplyStream[i]);
              if (DFSClient.LOG.isDebugEnabled()) {
                DFSClient.LOG.debug("DFSClient " + ack);
              }
              long seqno = ack.getSeqno();
              // processes response status from datanodes.
              for (int j = ack.getNumOfReplies() - 1; j >= 0
                  && dfsClient.clientRunning; j--) {
                final DataTransferProtocol.Status reply = ack.getReply(j);
                if (reply != SUCCESS) {
                  // 1-8 change by tianshan
                  // errorIndex = j; // first bad datanode
                  throw new IOException("Bad response " + reply + " for block "
                      + blocks[i] + " from datanode " + targets[i].getName());
                }
              }
              assert seqno != PipelineAck.UNKOWN_SEQNO : "Ack for unkown seqno should be a failed ack: "
                  + ack;
              if (seqno == Packet.HEART_BEAT_SEQNO) { // a heartbeat ack
                continue;
              }
              // a success ack for a data packet
              Packet one = null;
              synchronized (ackQueue.get(i)) {
                one = ackQueue.get(i).getFirst();
                ackQueue.get(i).notifyAll();
              }
              if (one.seqno != seqno) {
                throw new IOException("Responseprocessor: Expecting seqno "
                    + " for block " + blocks[i] + one.seqno + " but received "
                    + seqno);
              }
              isLastPacketInBlock = one.lastPacketInBlock;
              // update bytesAcked
              blocks[i].setNumBytes(one.getLastByteOffsetBlock());
              // DFSClient.LOG.info("block at index " + i + " SetNumbytes "
              // +one.getLastByteOffsetBlock());
              synchronized (ackQueue.get(i)) {
                lastAckedSeqno = seqno;
                ackQueue.get(i).removeFirst();
                ackQueue.get(i).notifyAll();
              }

            } catch (Exception e) {
              if (e instanceof EOFException) {
                continue;
              }
              // 1-8 change by tinashan
              // if (!responderClosed) {
              if (dnState[i]) {
                if (e instanceof IOException) {
                  setLastException((IOException) e);
                }
                hasError = true;
                errorIndex = errorIndex == -1 ? 0 : errorIndex;
                // synchronized (dataQueue) {
                // dataQueue.notifyAll();
                // }
                DFSClient.LOG
                    .warn("DFSOutputStream ResponseProcessor exception "
                        + " for block " + blocks[i]
                        + StringUtils.stringifyException(e));
//                responderClosed = true;
                dnState[i] = false;
              }
            }
          }
        }
      }

      void close() {
        responderClosed = true;
        this.interrupt();
      }
    }

    // If this stream has encountered any errors so far, shutdown
    // threads and mark stream as closed. Returns true if we should
    // sleep for a while after returning from this call.
    //
//    private boolean processDatanodeError() throws IOException {
//      if (response != null) {
//        DFSClient.LOG.info("Error Recovery for block " + src
//            + " waiting for responder to exit. ");
//        return true;
//      }
//      closeStream();
//
//      // move packets from ack queue to front of the data queue
//      // TODO:
//      synchronized (dataQueue) {
//        dataQueue.addAll(0, ackQueue);
//        ackQueue.clear();
//      }
//
//      boolean doSleep = setupPipelineForAppendOrRecovery();
//
//      if (!streamerClosed && dfsClient.clientRunning) {
//        if (stage == BlockConstructionStage.PIPELINE_CLOSE) {
//
//          // If we had an error while closing the pipeline, we go through a
//          // fast-path
//          // where the BlockReceiver does not run. Instead, the DataNode just
//          // finalizes
//          // the block immediately during the 'connect ack' process. So, we want
//          // to pull
//          // the end-of-block packet from the dataQueue, since we don't actually
//          // have
//          // a true pipeline to send it over.
//          //
//          // We also need to set lastAckedSeqno to the end-of-block Packet's
//          // seqno, so that
//          // a client waiting on close() will be aware that the flush finished.
//          synchronized (dataQueue) {
//            assert dataQueue.size() == 1;
//            // Packet endOfBlockPacket = dataQueue.remove(); // remove the end
//            // of block packet
//            // assert endOfBlockPacket.lastPacketInBlock;
//            // assert lastAckedSeqno == endOfBlockPacket.seqno - 1;
//            // lastAckedSeqno = endOfBlockPacket.seqno;
//            dataQueue.notifyAll();
//          }
//          endBlock();
//        } else {
//          initDataStreaming();
//        }
//      }
//
//      return doSleep;
//    }
    
    // 1-8 add by tianshan
    // If the i's stream has encountered any errors, 
    // just shutdown the i's stream, and record a recovery command
    private boolean processDatanodeError() throws IOException {
      
      // there is already one error
      if (dfsErrorIndex >=0 ) {
        // TODO: wheather need more handle
        DFSClient.LOG.info("DFSOutputStream err num more than 1:"+dfsErrorIndex);
        closeStream();
        return true;
      }
      
      // the error is in createBlockOutputStream
      if (blockReplyStream == null) {
        // TODO: wheather need more handle
        closeStream();
        return true;
      }
      
      // clear the dataQueue and ackQueue;
      synchronized (dataQueue.get(errorIndex)) {
        dataQueue.get(errorIndex).clear();
        ackQueue.get(errorIndex).clear();
      }
      // recover other transmit
      hasError = false;
      // int waitAndCodeCurrentPacket(),has a judge
      // wheather the lastExcepiton is null, so need to change it to null
      lastException = null;

      // 1-8 add by tianshan
      dnState[errorIndex] = false;
      dfsErrorIndex = errorIndex;
      errNode = nodes[errorIndex];
      
      DFSClient.LOG.info("has an exception on datanode "+errorIndex+" : "+nodes[errorIndex].toString());
      
      return false;
    }

    /**
     * Not suported recently. the port is reserved. Open a DataOutputStream to a
     * DataNode pipeline so that it can be written to. This happens when a file
     * is appended or data streaming fails It keeps on trying until a pipeline
     * is setup
     */
    private boolean setupPipelineForAppendOrRecovery() throws IOException {
      // // check number of datanodes
      // if (nodes == null || nodes.length == 0) {
      // String msg = "Could not get block locations. " + "Source file \""
      // + src + "\" - Aborting...";
      // DFSClient.LOG.warn(msg);
      // setLastException(new IOException(msg));
      // streamerClosed = true;
      // return false;
      // }
      //
      // boolean success = false;
      // long newGS = 0L;
      // while (!success && !streamerClosed && dfsClient.clientRunning) {
      // boolean isRecovery = hasError;
      // // remove bad datanode from list of datanodes.
      // // If errorIndex was not set (i.e. appends), then do not remove
      // // any datanodes
      // //
      // if (errorIndex >= 0) {
      // StringBuilder pipelineMsg = new StringBuilder();
      // for (int j = 0; j < nodes.length; j++) {
      // pipelineMsg.append(nodes[j].getName());
      // if (j < nodes.length - 1) {
      // pipelineMsg.append(", ");
      // }
      // }
      // if (nodes.length <= 1) {
      // lastException = new IOException("All datanodes " + pipelineMsg
      // + " are bad. Aborting...");
      // streamerClosed = true;
      // return false;
      // }
      // DFSClient.LOG.warn("Error Recovery for block " + block +
      // " in pipeline " + pipelineMsg +
      // ": bad datanode " + nodes[errorIndex].getName());
      // DatanodeInfo[] newnodes = new DatanodeInfo[nodes.length-1];
      // System.arraycopy(nodes, 0, newnodes, 0, errorIndex);
      // System.arraycopy(nodes, errorIndex+1, newnodes, errorIndex,
      // newnodes.length-errorIndex);
      // nodes = newnodes;
      // hasError = false;
      // lastException = null;
      // errorIndex = -1;
      // }
      //
      // // get a new generation stamp and an access token
      // LocatedBlock lb = dfsClient.namenode.updateBlockForPipeline(block,
      // dfsClient.clientName);
      // newGS = lb.getBlock().getGenerationStamp();
      // accessToken = lb.getBlockToken();
      //
      // // set up the pipeline again with the remaining nodes
      // // Not supported recently.
      // // success = createBlockOutputStream(nodes, newGS, isRecovery);
      // }
      //
      // if (success) {
      // // update pipeline at the namenode
      // Block newBlock = new Block(
      // block.getBlockId(), block.getNumBytes(), newGS);
      // dfsClient.namenode.updatePipeline(dfsClient.clientName, block,
      // newBlock, nodes);
      // // update client side generation stamp
      // block = newBlock;
      // }
      // return false; // do not sleep, continue processing
      return false;
    }

    /**
     * Open a DataOutputStream to a DataNode so that it can be written to. This
     * happens when a file is created and each time a new block is allocated.
     * Must get block ID and the IDs of the destinations from the namenode.
     * Returns the list of target datanodes.
     */
    private DatanodeInfo[] nextBlockOutputStream(String client)
        throws IOException {
      LocatedBlock[] lb = null;
      /**
       * 
       */
      // DatanodeInfo[] nodes = null;
      LinkedList<DatanodeInfo> result = new LinkedList<DatanodeInfo>();
      int count = conf.getInt("dfs.client.block.write.retries", 3);
      boolean success = false;
      do {
        hasError = false;
        lastException = null;
        errorIndex = -1;
        success = false;

        long startTime = System.currentTimeMillis();
        DatanodeInfo[] w = excludedNodes.toArray(new DatanodeInfo[excludedNodes
            .size()]);
        lb = locateFollowingBlock(startTime, w.length > 0 ? w : null);
        for (int i = 0; i < lb.length; ++i) {
          for (DatanodeInfo d : lb[i].getLocations()) {
            result.add(d);
          }
          blocks[i] = lb[i].getBlock();
          blocks[i].setNumBytes(0);
          /**
           * TODO
           */
          // block = l.getBlock();
          // block.setNumBytes(0);
        }
        accessToken = lb[0].getBlockToken();
        //
        // Connect to first DataNode in the list.
        //
        for (int i = 0; i < blocks.length; ++i) {
          // DFSClient.LOG.info("Block at index of " + i +" :" +
          // blocks[i].toString());
        }
        success = createBlockOutputStream(result, 0L, false);
        // TODO:
        // What shall we do when failure?
        if (!success) {
          DFSClient.LOG.info("Abandoning file " + src);
          // dfsClient.namenode.abandonBlock(block, src, dfsClient.clientName);
          // block = null;
          // DFSClient.LOG.info("Excluding datanode " + nodes[errorIndex]);
          // excludedNodes.add(nodes[errorIndex]);
        }
      } while (!success && --count >= 0);

      if (!success) {
        throw new IOException("Unable to create new block.");
      }
      return result.toArray(new DatanodeInfo[0]);
    }

    // connects to the first datanode in the pipeline
    // Returns true if success, otherwise return failure.
    //
    // private boolean createBlockOutputStream(DatanodeInfo[] nodes, long newGS,
    // boolean recoveryFlag) {
    // DataTransferProtocol.Status pipelineStatus = SUCCESS;
    // String firstBadLink = "";
    // if (DFSClient.LOG.isDebugEnabled()) {
    // for (int i = 0; i < nodes.length; i++) {
    // DFSClient.LOG.debug("pipeline = " + nodes[i].getName());
    // }
    // }
    //
    // // persist blocks on namenode on next flush
    // persistBlocks.set(true);
    //
    // boolean result = false;
    // try {
    // if(DFSClient.LOG.isDebugEnabled()) {
    // DFSClient.LOG.debug("Connecting to " + nodes[0].getName());
    // }
    // InetSocketAddress target = NetUtils.createSocketAddr(nodes[0].getName());
    // s = dfsClient.socketFactory.createSocket();
    // int timeoutValue = dfsClient.getDatanodeReadTimeout(nodes.length);
    // NetUtils.connect(s, target, timeoutValue);
    // s.setSoTimeout(timeoutValue);
    // s.setSendBufferSize(DFSClient.DEFAULT_DATA_SOCKET_SIZE);
    // if(DFSClient.LOG.isDebugEnabled()) {
    // DFSClient.LOG.debug("Send buf size " + s.getSendBufferSize());
    // }
    // long writeTimeout = dfsClient.getDatanodeWriteTimeout(nodes.length);
    //
    // //
    // // Xmit header info to datanode
    // //
    // DataOutputStream out = new DataOutputStream(new BufferedOutputStream(
    // NetUtils.getOutputStream(s, writeTimeout),
    // DataNode.SMALL_BUFFER_SIZE));
    // blockReplyStream = new DataInputStream(NetUtils.getInputStream(s));
    //
    // // send the request
    // DataTransferProtocol.Sender.opWriteBlock(out, block, nodes.length,
    // recoveryFlag ? stage.getRecoveryStage() : stage, newGS,
    // block.getNumBytes(), bytesSent, dfsClient.clientName, null, nodes,
    // accessToken);
    // checksum.writeHeader(out);
    // out.flush();
    //
    // // receive ack for connect
    // pipelineStatus = DataTransferProtocol.Status.read(blockReplyStream);
    // firstBadLink = Text.readString(blockReplyStream);
    // if (pipelineStatus != SUCCESS) {
    // if (pipelineStatus == ERROR_ACCESS_TOKEN) {
    // throw new InvalidBlockTokenException(
    // "Got access token error for connect ack with firstBadLink as "
    // + firstBadLink);
    // } else {
    // throw new IOException("Bad connect ack with firstBadLink as "
    // + firstBadLink);
    // }
    // }
    //
    // blockStream = out;
    // result = true; // success
    //
    // } catch (IOException ie) {
    //
    // DFSClient.LOG.info("Exception in createBlockOutputStream " + ie);
    //
    // // find the datanode that matches
    // if (firstBadLink.length() != 0) {
    // for (int i = 0; i < nodes.length; i++) {
    // if (nodes[i].getName().equals(firstBadLink)) {
    // errorIndex = i;
    // break;
    // }
    // }
    // } else {
    // errorIndex = 0;
    // }
    // hasError = true;
    // setLastException(ie);
    // blockReplyStream = null;
    // result = false; // error
    // } finally {
    // if (!result) {
    // IOUtils.closeSocket(s);
    // s = null;
    // }
    // }
    // return result;
    // }
    /**
     * @author vither
     * @param nodes
     * @param newGS
     * @param recoveryFlag
     * @return
     */
    private boolean createBlockOutputStream(LinkedList<DatanodeInfo> datanodes,
        long newGS, boolean recoveryFlag) {
      DataTransferProtocol.Status pipelineStatus = SUCCESS;
      String firstBadLink = "";
      DatanodeInfo[] nodes = datanodes.toArray(new DatanodeInfo[0]);
      DatanodeInfo[] dn = new DatanodeInfo[1];
      if (DFSClient.LOG.isDebugEnabled()) {
        for (int i = 0; i < nodes.length; i++) {
          DFSClient.LOG.debug("Connection = " + nodes[i].getName());
        }
      }

      // persist blocks on namenode on next flush
      persistBlocks.set(true);

      boolean result = true;
      for (int i = 0; i < nodes.length; ++i) {
        try {
          if (DFSClient.LOG.isDebugEnabled()) {
            DFSClient.LOG.debug("Connecting to " + nodes[i].getName());
          }
          InetSocketAddress target = NetUtils.createSocketAddr(nodes[i]
              .getName());
          s[i] = dfsClient.socketFactory.createSocket();
          int timeoutValue = dfsClient.getDatanodeReadTimeout(1);
          NetUtils.connect(s[i], target, timeoutValue);
          s[i].setSoTimeout(timeoutValue);
          s[i].setSendBufferSize(DFSClient.DEFAULT_DATA_SOCKET_SIZE);
          if (DFSClient.LOG.isDebugEnabled()) {
            DFSClient.LOG.debug("Send buf size " + s[i].getSendBufferSize());
          }
          long writeTimeout = dfsClient.getDatanodeWriteTimeout(1);

          //
          // Xmit header info to datanode
          //
          blockStream[i] = new DataOutputStream(new BufferedOutputStream(
              NetUtils.getOutputStream(s[i], writeTimeout),
              DataNode.SMALL_BUFFER_SIZE));
          blockReplyStream[i] = new DataInputStream(
              NetUtils.getInputStream(s[i]));

          // send the request
          dn[0] = nodes[i];
          DataTransferProtocol.Sender.opWriteBlock(blockStream[i], blocks[i],
              1, recoveryFlag ? stage.getRecoveryStage() : stage, newGS,
              blocks[i].getNumBytes(), bytesSent, dfsClient.clientName, null,
              dn, accessToken);
          checksum.writeHeader(blockStream[i]);
          blockStream[i].flush();

          // receive ack for connect
          pipelineStatus = DataTransferProtocol.Status
              .read(blockReplyStream[i]);
          firstBadLink = Text.readString(blockReplyStream[i]);
          if (pipelineStatus != SUCCESS) {
            if (pipelineStatus == ERROR_ACCESS_TOKEN) {
              throw new InvalidBlockTokenException(
                  "Got access token error for connect ack with firstBadLink as "
                      + firstBadLink);
            } else {
              throw new IOException("Bad connect ack with firstBadLink as "
                  + firstBadLink);
            }
          }
          result |= true; // success

        } catch (IOException ie) {

          DFSClient.LOG.info("Exception in createBlockOutputStream " + ie);

          // find the datanode that matches
          if (firstBadLink.length() != 0) {
            if (nodes[i].getName().equals(firstBadLink)) {
              errorIndex = i;
            }
          } else {
            errorIndex = 0;
          }
          hasError = true;
          setLastException(ie);
          blockReplyStream = null;
          result = false; // error
        } finally {
          if (!result) {
            IOUtils.closeSocket(s[i]);
            s[i] = null;
          }
        }
      }

      return result;
    }

    private LocatedBlock[] locateFollowingBlock(long start,
        DatanodeInfo[] excludedNodes) throws IOException,
        UnresolvedLinkException {
      int retries = conf.getInt(
          "dfs.client.block.write.locateFollowingBlock.retries", 5);
      long sleeptime = 400;
      while (true) {
        long localstart = System.currentTimeMillis();
        while (true) {
          try {
            return dfsClient.namenode.addBlock(src, dfsClient.clientName, null,
                excludedNodes);
          } catch (RemoteException e) {
            IOException ue = e.unwrapRemoteException(
                FileNotFoundException.class, AccessControlException.class,
                NSQuotaExceededException.class, DSQuotaExceededException.class,
                UnresolvedPathException.class);
            if (ue != e) {
              throw ue; // no need to retry these exceptions
            }

            if (NotReplicatedYetException.class.getName().equals(
                e.getClassName())) {
              if (retries == 0) {
                throw e;
              } else {
                --retries;
                DFSClient.LOG.info(StringUtils.stringifyException(e));
                if (System.currentTimeMillis() - localstart > 5000) {
                  DFSClient.LOG.info("Waiting for replication for "
                      + (System.currentTimeMillis() - localstart) / 1000
                      + " seconds");
                }
                try {
                  DFSClient.LOG.warn("NotReplicatedYetException sleeping "
                      + src + " retries left " + retries);
                  Thread.sleep(sleeptime);
                  sleeptime *= 2;
                } catch (InterruptedException ie) {
                }
              }
            } else {
              throw e;
            }

          }
        }
      }
    }

    Block[] getBlocks() {
      return blocks;
    }

    DatanodeInfo[] getNodes() {
      return nodes;
    }

    Token<BlockTokenIdentifier> getBlockToken() {
      return accessToken;
    }

    private void setLastException(IOException e) {
      if (lastException == null) {
        lastException = e;
      }
    }

    // class miniSend implements Runnable{
    //
    // private int i ;
    //
    // public miniSend(int i){
    // this.i = i;
    // }
    //
    // @Override
    // public void run() {
    // // TODO Auto-generated method stub
    // long lastPacket = System.currentTimeMillis();
    //
    // try{
    // boolean doSleep = false;
    // if (hasError && errorIndex>=0) {
    // doSleep = processDatanodeError();
    // }
    //
    // Packet one = null;
    //
    // while (!streamerClosed && dfsClient.clientRunning) {
    //
    // synchronized (dataQueue.get(i)) {
    // // wait for a packet to be sent.
    // long now = System.currentTimeMillis();
    // while ((!streamerClosed && !hasError && dfsClient.clientRunning
    // && dataQueue.get(i).isEmpty() && !queueEnd[i] &&
    // (stage != BlockConstructionStage.DATA_STREAMING ||
    // stage == BlockConstructionStage.DATA_STREAMING &&
    // now - lastPacket < dfsClient.socketTimeout/2)) || doSleep ) {
    // long timeout = dfsClient.socketTimeout/2 - (now-lastPacket);
    // timeout = timeout <= 0 ? 1000 : timeout;
    // timeout = (stage == BlockConstructionStage.DATA_STREAMING)?
    // timeout : 1000;
    // try {
    // dataQueue.get(i).wait(timeout);
    // } catch (InterruptedException e) {
    // }
    // doSleep = false;
    // now = System.currentTimeMillis();
    // }
    // if (streamerClosed || hasError || !dfsClient.clientRunning) {
    // continue;
    // }
    // if (dataQueue.get(i).isEmpty()) {
    // continue;
    // //one = new Packet(); // heartbeat packet
    // } else {
    //
    // one = dataQueue.get(i).getFirst(); // regular data packet
    // // DFSClient.LOG.info("Getting packet from dataQueue at index:" + i
    // // + "offset=" + one.offsetInBlock);
    // }
    // assert one != null;
    //
    // // long lastByteOffsetInBlock = one.getLastByteOffsetBlock();
    // // if (lastByteOffsetInBlock > blockSize) {
    // // throw new IOException("BlockSize " + blockSize +
    // // " is smaller than data size. " +
    // // " Offset of packet in block " +
    // // lastByteOffsetInBlock +
    // // " Aborting file " + src);
    // // }
    //
    // // if (one.lastPacketInBlock) {
    // // // wait for all data packets have been successfully acked
    // // synchronized (dataQueue.get(i)) {
    // // while (!streamerClosed && !hasError &&
    // // ackQueue.get(i).size() != 0 && dfsClient.clientRunning) {
    // // try {
    // // // wait for acks to arrive from datanodes
    // // dataQueue.get(i).wait(1000);
    // // } catch (InterruptedException e) {
    // // }
    // // }
    // // }
    // // if (streamerClosed || hasError || !dfsClient.clientRunning) {
    // // continue;
    // // }
    // // if (--num == 0) {
    // // stage = BlockConstructionStage.PIPELINE_CLOSE;
    // // }
    // //
    // // }
    //
    // // send the packet
    // ByteBuffer buf = one.getBuffer();
    //
    // if (!one.isHeartbeatPacket()) {
    // synchronized (dataQueue.get(i)) {
    // // move packet from dataQueue to ackQueue
    // dataQueue.get(i).removeFirst();
    // ackQueue.get(i).addLast(one);
    // dataQueue.get(i).notifyAll();
    // }
    // }
    //
    // if (DFSClient.LOG.isDebugEnabled()) {
    // DFSClient.LOG.debug("DataStreamer block " + blocks[i] +
    // " sending packet " + one);
    // }
    //
    // // write out data to remote datanode
    // blockStream[i].write(buf.array(), buf.position(), buf.remaining());
    // blockStream[i].flush();
    // lastPacket = System.currentTimeMillis();
    // if (one.isHeartbeatPacket()) { //heartbeat packet
    // }
    // // update bytesSent
    // long tmpBytesSent = one.getLastByteOffsetBlock();
    // if (bytesSent < tmpBytesSent) {
    // bytesSent = tmpBytesSent;
    // }
    // if (streamerClosed || hasError || !dfsClient.clientRunning) {
    // continue;
    // }
    // if (one.lastPacketInBlock) {
    // // wait for the close packet has been acked
    // --num;
    // queueEnd[i] = true;
    // if(num == 0){
    // synchronized (dataQueue.get(i)) {
    // int ackSize = 0;
    // for (LinkedList<Packet> aq : ackQueue) {
    // ackSize += aq.size();
    // }
    // while (!streamerClosed && !hasError &&
    // ackSize != 0 && dfsClient.clientRunning) {
    // dataQueue.get(i).wait(1000);// wait for acks to arrive from datanodes
    // }
    // }
    // if (streamerClosed || hasError || !dfsClient.clientRunning) {
    // continue;
    // }
    // endBlock();
    // stage = BlockConstructionStage.PIPELINE_CLOSE;
    // }
    // }
    // if (progress != null) { progress.progress(); }
    //
    // // This is used by unit test to trigger race conditions.
    // if (artificialSlowdown != 0 && dfsClient.clientRunning) {
    // Thread.sleep(artificialSlowdown);
    // }
    // }
    // }
    // }catch (Exception e) {
    //
    // DFSClient.LOG.warn("miniSend Exception: " +
    // StringUtils.stringifyException(e));
    // if (e instanceof IOException) {
    // setLastException((IOException)e);
    // }
    // hasError = true;
    // if (errorIndex == -1) { // not a datanode error
    // streamerClosed = true;
    // }
    //
    // }
    //
    // }
    //
    // }
  }

  private void isClosed() throws IOException {
    if (closed) {
      IOException e = lastException;
      throw e != null ? e : new IOException("DFSOutputStream is closed");
    }
  }

  //
  // returns the list of targets, if any, that is being currently used.
  //
  synchronized DatanodeInfo[] getPipeline() {
    if (streamer == null) {
      return null;
    }
    DatanodeInfo[] currentNodes = streamer.getNodes();
    if (currentNodes == null) {
      return null;
    }
    DatanodeInfo[] value = new DatanodeInfo[currentNodes.length];
    for (int i = 0; i < currentNodes.length; i++) {
      value[i] = currentNodes[i];
    }
    return value;
  }

  // TODO:
  private DFSOutputStream(DFSClient dfsClient, String src, long blockSize,
      Progressable progress, int bytesPerChecksum, short replication)
      throws IOException {
    super(new PureJavaCrc32(), bytesPerChecksum, 4);
    this.dfsClient = dfsClient;
    this.conf = dfsClient.conf;
    this.src = src;
    // this.blockSize = blockSize;
    this.blockReplication = replication;
    this.progress = progress;
    if ((progress != null) && DFSClient.LOG.isDebugEnabled()) {
      DFSClient.LOG.debug("Set non-null progress callback on DFSOutputStream "
          + src);
    }

    if (bytesPerChecksum < 1 || blockSize % bytesPerChecksum != 0) {
      throw new IOException("io.bytes.per.checksum(" + bytesPerChecksum
          + ") and blockSize(" + blockSize + ") do not match. "
          + "blockSize should be a " + "multiple of io.bytes.per.checksum");

    }
    checksum = DataChecksum.newDataChecksum(DataChecksum.CHECKSUM_CRC32,
        bytesPerChecksum);
  }

  // TODO:
  private DFSOutputStream(DFSClient dfsClient, String src, long blockSize,
      Progressable progress, int bytesPerChecksum) throws IOException {
    super(new PureJavaCrc32(), bytesPerChecksum, 4);
    this.dfsClient = dfsClient;
    this.conf = dfsClient.conf;
    this.src = src;
    // this.blockSize = blockSize;
    this.progress = progress;
    if ((progress != null) && DFSClient.LOG.isDebugEnabled()) {
      DFSClient.LOG.debug("Set non-null progress callback on DFSOutputStream "
          + src);
    }

    if (bytesPerChecksum < 1 || blockSize % bytesPerChecksum != 0) {
      throw new IOException("io.bytes.per.checksum(" + bytesPerChecksum
          + ") and blockSize(" + blockSize + ") do not match. "
          + "blockSize should be a " + "multiple of io.bytes.per.checksum");

    }
    checksum = DataChecksum.newDataChecksum(DataChecksum.CHECKSUM_CRC32,
        bytesPerChecksum);
  }

  /**
   * Create a new output stream to the given DataNode.
   * 
   * @see ClientProtocol#create(String, FsPermission, String, EnumSetWritable,
   *      boolean, short, long)
   */
  DFSOutputStream(DFSClient dfsClient, String src, FsPermission masked,
      EnumSet<CreateFlag> flag, boolean createParent, short replication,
      long blockSize, Progressable progress, int buffersize,
      int bytesPerChecksum) throws IOException {
    this(dfsClient, src, blockSize, progress, bytesPerChecksum, replication);

    computePacketChunkSize(dfsClient.writePacketSize, bytesPerChecksum);
    try {
      dfsClient.namenode.create(src, masked, dfsClient.clientName,
          new EnumSetWritable<CreateFlag>(flag), createParent, replication,
          blockSize);
    } catch (RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
          DSQuotaExceededException.class, FileAlreadyExistsException.class,
          FileNotFoundException.class, ParentNotDirectoryException.class,
          NSQuotaExceededException.class, SafeModeException.class,
          UnresolvedPathException.class);
    }
    streamer = new DataStreamer();
    streamer.start();
  }

  DFSOutputStream(DFSClient dfsClient, String src, long size,
      CodingMatrix matrix, FsPermission masked, EnumSet<CreateFlag> flag,
      boolean createParent, Progressable progress, int buffersize,
      int bytesPerChecksum) throws IOException {
    this(dfsClient, src, 0, progress, bytesPerChecksum);
    this.fileSize = size;
    this.matrix = matrix;
    // computePacketChunkSize(dfsClient.writePacketSize, bytesPerChecksum);
    int n = matrix.getColumn();
    s = new Socket[n];
    blocks = new Block[n];
    dataQueue = new ArrayList<LinkedList<Packet>>(n);
    ackQueue = new ArrayList<LinkedList<Packet>>(n);
    codeBuf = new Packet[n];
    queueEnd = new boolean[n];
    for (int i = 0; i < n; ++i) {
      dataQueue.add(i, new LinkedList<Packet>());
      ackQueue.add(i, new LinkedList<Packet>());
      codeBuf[i] = null;
      queueEnd[i] = false;
    }

    // 1-8 add by tianshan
    // init the error record variable
    dnState = new boolean[matrix.getColumn()];
    for (int i = 0; i < dnState.length; i++)
      dnState[i] = true;
    dfsErrorIndex = -1;
    errNode = null;
    // end of init error record variable

    streamer = new DataStreamer();
    streamer.start();
    pool = Executors.newFixedThreadPool(3 * n);
  }

  /**
   * Create a new output stream to the given DataNode.
   * 
   * @see ClientProtocol#create(String, FsPermission, String, boolean, short,
   *      long)
   */
  DFSOutputStream(DFSClient dfsClient, String src, int buffersize,
      Progressable progress, LocatedBlock lastBlock, HdfsFileStatus stat,
      int bytesPerChecksum) throws IOException {
    this(dfsClient, src, stat.getBlockSize(), progress, bytesPerChecksum, stat
        .getReplication());
    initialFileSize = stat.getLen(); // length of file when opened

    //
    // The last partial block of the file has to be filled.
    //
    if (lastBlock != null) {
      // indicate that we are appending to an existing block
      bytesCurBlock = lastBlock.getBlockSize();
      streamer = new DataStreamer(lastBlock, stat, bytesPerChecksum);
    } else {
      computePacketChunkSize(dfsClient.writePacketSize, bytesPerChecksum);
      streamer = new DataStreamer();
    }
    streamer.start();
  }

  private void computePacketChunkSize(int psize, int csize) {
    int chunkSize = csize + checksum.getChecksumSize();
    int max = (dfsClient.writePacketSize - PacketHeader.PKT_HEADER_LEN
        + chunkSize - 1)
        / chunkSize;
    if (psize > max * csize) {
      chunksPerPacket = max;
    } else {
      chunksPerPacket = (psize + csize - 1) / csize;
    }

    packetSize = PacketHeader.PKT_HEADER_LEN + chunkSize * chunksPerPacket;
    /*
     * DFSClient.LOG.info("computePacketChunkSize: pszie=" + psize +
     * ", chunksPerPacket=" + chunksPerPacket+ "chunkSize="+chunkSize +
     * " //////"+checksum.getChecksumSize()+"...."+csize);
     */
    if (DFSClient.LOG.isDebugEnabled()) {
      DFSClient.LOG.debug("computePacketChunkSize: src=" + src + ", chunkSize="
          + chunkSize + ", chunksPerPacket=" + chunksPerPacket
          + ", packetSize=" + packetSize);
    }
  }

  // private void codeCurrentPacket() {
  // synchronized (dataQueue) {
  // if (currentPacket == null) return;
  // for (LinkedList<Packet> dq : dataQueue) {
  // dq.addLast(currentPacket);
  // }
  //
  // lastQueuedSeqno = currentPacket.seqno;
  // if (DFSClient.LOG.isDebugEnabled()) {
  // DFSClient.LOG.debug("Queued packet " + currentPacket.seqno);
  // }
  // currentPacket = null;
  // dataQueue.notifyAll();
  // }
  // }

  private void codeCurrentPacket() {

    if (currentPacket == null)
      return;

    if (currentPacket.lastPacketInBlock == true) {

      // DFSClient.LOG.info("lastPacketInBlock!");

      chckSumCount = 0;
      for (int i = 0; i < codeBuf.length; ++i) {

        // DFSClient.LOG.info("offsetInb = " + offsetInB);

        if (codeBuf[i] == null) {
          // DFSClient.LOG.info("codBuf " + i +"is null");

          Packet cPacket = new Packet(PacketHeader.PKT_HEADER_LEN, 0,
              bytesCurBlock);
          cPacket.lastPacketInBlock = true;

          cPacket.setOffset(offsetInB);
          synchronized (dataQueue.get(i)) {
            dataQueue.get(i).add(cPacket);
            dataQueue.get(i).notify();
          }

          // DFSClient.LOG.info("seqo :"+currentPacket.seqno +
          // ", offsetInBlock :"+currentPacket.offsetInBlock +
          // ", lastPacketInBlock :"+currentPacket.lastPacketInBlock+
          // ", checksumPos:"+currentPacket.checksumPos +
          // ", checksumStart :"+currentPacket.checksumStart +
          // ", checksumlen :"+ (currentPacket.checksumPos
          // -currentPacket.checksumStart) +
          // ", dataPos :"+currentPacket.dataPos +
          // ", dataStart :"+currentPacket.dataStart+
          // ", checksumlen :"+ (currentPacket.dataPos
          // -currentPacket.dataStart)+
          // ", sum :"+ (currentPacket.dataPos -currentPacket.checksumStart));

        } else {
          // DFSClient.LOG.info("codBuf " + i +"is not null");
          // synchronized (dataQueue.get(i)) {
          // dataQueue.get(i).addLast(codeBuf[i]);
          // dataQueue.get(i).notify();
          // }

          // DFSClient.LOG.info("seqo :"+codeBuf[i].seqno +
          // ", offsetInBlock :"+codeBuf[i].offsetInBlock +
          // ", lastPacketInBlock :"+codeBuf[i].lastPacketInBlock+
          // ", checksumPos:"+codeBuf[i].checksumPos +
          // ", checksumStart :"+codeBuf[i].checksumStart +
          // ", checksumlen :"+ (codeBuf[i].checksumPos
          // -codeBuf[i].checksumStart) +
          // ", dataPos :"+codeBuf[i].dataPos +
          // ", dataStart :"+codeBuf[i].dataStart +
          // ", checksumlen :"+ (codeBuf[i].dataPos -codeBuf[i].dataStart)+
          // ", sum :"+ (codeBuf[i].dataPos -codeBuf[i].checksumStart));

          // currentPacket = new Packet(PacketHeader.PKT_HEADER_LEN, 0,
          // bytesCurBlock);
          // currentPacket.lastPacketInBlock = true;
          //
          // currentPacket.setOffset(offsetInB + codeBuf[i].dataPos -
          // codeBuf[i].dataStart);
          /*
           * DFSClient.LOG.info("i: "+i+"  dataPos: "+codeBuf[i].dataPos+
           * " dataStart:  "+codeBuf[i].dataStart + " seqno: " +
           * currentPacket.seqno + " BytescurrentBlock: " + bytesCurBlock);
           */
          // synchronized (dataQueue.get(i)) {
          // dataQueue.get(i).add(currentPacket);
          // dataQueue.get(i).notify();
          //
          // }

          // DFSClient.LOG.info("seqo :"+currentPacket.seqno +
          // ", offsetInBlock :"+currentPacket.offsetInBlock +
          // ", lastPacketInBlock :"+currentPacket.lastPacketInBlock+
          // ", checksumPos:"+currentPacket.checksumPos +
          // ", checksumStart :"+currentPacket.checksumStart +
          // ", checksumlen :"+ (currentPacket.checksumPos
          // -currentPacket.checksumStart) +
          // ", dataPos :"+currentPacket.dataPos +
          // ", dataStart :"+currentPacket.dataStart+
          // ", checksumlen :"+ (currentPacket.dataPos
          // -currentPacket.dataStart)+
          // ", sum :"+ (currentPacket.dataPos -currentPacket.checksumStart));

          pool.execute(new generateCheckSum(i, true));
          synchronized (waitCheckSum) {
            chckSumCount++;
            waitCheckSum.notifyAll();
          }

        }

      }

      while (true) {
        synchronized (waitCheckSum) {
          if (chckSumCount != 0) {
            try {
              waitCheckSum.wait();
              waitCheckSum.notifyAll();
            } catch (InterruptedException e) {
              // TODO Auto-generated catch block
              e.printStackTrace();
            }
          } else {
            break;
          }
        }
      }

      return;
    }

    lastQueuedSeqno = currentPacket.seqno;
    int row = (int) (lastQueuedSeqno % matrix.getRow());
    int column = matrix.getColumn();
    if (lastQueuedSeqno != 0 && row == 0) {

      chckSumCount = 0;
      for (int j = 0; j < column; ++j) {
        pool.execute(new generateCheckSum(j, false));
        synchronized (waitCheckSum) {
          chckSumCount++;
          waitCheckSum.notifyAll();
        }
        // synchronized (dataQueue.get(j)) {
        // dataQueue.get(j).addLast(codeBuf[j]);
        // dataQueue.get(j).notify();
        // }

        // DFSClient.LOG.info("seqo :"+codeBuf[j].seqno +
        // ", offsetInBlock :"+codeBuf[j].offsetInBlock +
        // ", lastPacketInBlock :"+codeBuf[j].lastPacketInBlock+
        // ", checksumPos:"+codeBuf[j].checksumPos +
        // ", checksumStart :"+codeBuf[j].checksumStart +
        // ", checksumlen :"+ (codeBuf[j].checksumPos -codeBuf[j].checksumStart)
        // +
        // ", dataPos :"+codeBuf[j].dataPos +
        // ", dataStart :"+codeBuf[j].dataStart+
        // ", checksumlen :"+ (codeBuf[j].dataPos -codeBuf[j].dataStart)+
        // ", sum :"+ (codeBuf[j].dataPos -codeBuf[j].checksumStart));

        // codeBuf[j] = null;
        if (DFSClient.LOG.isDebugEnabled()) {
          DFSClient.LOG.debug("Queued packet " + currentPacket.seqno);
        }
      }

      while (true) {
        synchronized (waitCheckSum) {
          if (chckSumCount != 0) {
            try {
              waitCheckSum.wait();
              waitCheckSum.notifyAll();
            } catch (InterruptedException e) {
              // TODO Auto-generated catch block
              e.printStackTrace();
            }
          } else {
            break;
          }
        }
      }
      offsetInB += 65024;
    }

    // long start = System.nanoTime();

    count = 0;
    // seq LCTBLK.4 3
	// added by ds at 2014-5-7
	boolean isRCR = RegeneratingCodeMatrix.isRegeneratingCodeRecovery();
	for (int i = 0; i < matrix.getColumn(); ++i)
	{
		// seq LCTBLK.4 4
		// modified by ds at 2014-5-7
		// modified modified by ds begins
		// //if (matrix.getElemAt(row, i) != 0)
		if (isRCR || matrix.getElemAt(row, i) != 0)
		{
			synchronized (object)
			{
				count++;
				object.notify();
			}
			if (codeBuf[i] == null)
			{
				codeBuf[i] = new Packet(currentPacket);
				// codeBuf[i].mult(matrix.getElemAt(row, i));
				// new Thread(new mult(codeBuf[i], matrix.getElemAt(row,
				// i))).start();;
				pool.execute(new mult(codeBuf[i], matrix.getElemAt(row, i)));
				codeBuf[i].setOffset(offsetInB);
			}
			else
			{
				// codeBuf[i].code(currentPacket,matrix.getElemAt(row, i));
				// new Thread(new code(codeBuf[i], currentPacket,
				// matrix.getElemAt(row, i))).start();
				pool.execute(new code(codeBuf[i], currentPacket, matrix.getElemAt(row, i)));
			}

		}

	}

    // DFSClient.LOG.info("before wait: "+(System.nanoTime()-start));
    while (true) {
      synchronized (object) {
        if (count != 0) {
          try {
            object.wait();
            object.notifyAll();
          } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
          }
        } else {
          break;
        }
      }
    }

    // DFSClient.LOG.info("out:  "+(System.nanoTime()-start));
    currentPacket = null;

  }

  // private void waitAndQueueCurrentPacket() throws IOException {
  // synchronized (dataQueue) {
  // // If queue is full, then wait till we have enough space
  // while (!closed && dataQueue.size() + ackQueue.size() > MAX_PACKETS) {
  // try {
  // dataQueue.wait();
  // } catch (InterruptedException e) {
  // // If we get interrupted while waiting to queue data, we still need to get
  // rid
  // // of the current packet. This is because we have an invariant that if
  // // currentPacket gets full, it will get queued before the next writeChunk.
  // //
  // // Rather than wait around for space in the queue, we should instead try to
  // // return to the caller as soon as possible, even though we slightly
  // overrun
  // // the MAX_PACKETS iength.
  // Thread.currentThread().interrupt();
  // break;
  // }
  // }
  // isClosed();
  // queueCurrentPacket();
  // }
  // }
  private void waitAndCodeCurrentPacket() throws IOException {
    // If queue is full, then wait till we have enough space
    // while (!closed && dataQueue.size() + ackQueue.size() > MAX_PACKETS)
    // TODO:
    // DFSClient.LOG.info("waitAndCodeCurrentPacket()");
    // TODO:
    while (!closed && dataQueue.get(0).size() > MAX_PACKETS) {

    }
    isClosed();
    codeCurrentPacket();
  }

  // @see FSOutputSummer#writeChunk()
  @Override
  protected synchronized void writeChunk(byte[] b, int offset, int len,
      byte[] checksum) throws IOException {

    dfsClient.checkOpen();
    isClosed();
    int cklen = checksum.length;
    int bytesPerChecksum = this.checksum.getBytesPerChecksum();

    if (currentPacket == null || currentPacket.numChunks == 0) {
      if (appendChunk && bytesCurBlock % bytesPerChecksum == 0) {
        appendChunk = false;
        resetChecksumChunk(bytesPerChecksum);
      }

      if (!appendChunk) {
        int psize = (int) Math.min((fileSize - bytesCurBlock),
            (long) dfsClient.writePacketSize);
        computePacketChunkSize(psize, bytesPerChecksum);
        // DFSClient.LOG.info("chunksPerPacket=" + chunksPerPacket +" psize="
        // +psize);
      }
    }

    if (len > bytesPerChecksum) {
      throw new IOException("writeChunk() buffer size is " + len
          + " is larger than supported  bytesPerChecksum " + bytesPerChecksum);
    }
    if (checksum.length != this.checksum.getChecksumSize()) {
      throw new IOException("writeChunk() checksum size is supposed to be "
          + this.checksum.getChecksumSize() + " but found to be "
          + checksum.length);
    }

    if (currentPacket == null) {
      currentPacket = new Packet(packetSize, chunksPerPacket, bytesCurBlock);
      if (DFSClient.LOG.isDebugEnabled()) {
        DFSClient.LOG.debug("DFSClient writeChunk allocating new packet seqno="
            + currentPacket.seqno + ", src=" + src + ", packetSize="
            + packetSize + ", chunksPerPacket=" + chunksPerPacket
            + ", bytesCurBlock=" + bytesCurBlock);
      }
      /*
       * DFSClient.LOG.info("DFSClient writeChunk allocating new packet seqno="
       * + currentPacket.seqno + ", src=" + src + ", packetSize=" + packetSize +
       * ", chunksPerPacket=" + chunksPerPacket + ", bytesCurBlock=" +
       * bytesCurBlock);
       */
    }

    // currentPacket.writeChecksum(checksum, 0, cklen);
    currentPacket.writeData(b, offset, len);
    currentPacket.numChunks++;
    bytesCurBlock += len;

    // If packet is full, enqueue it for transmission
    //
    if (currentPacket.numChunks == currentPacket.maxChunks
        || bytesCurBlock == fileSize) {
      if (DFSClient.LOG.isDebugEnabled()) {
        DFSClient.LOG.debug("DFSClient writeChunk packet full seqno="
            + currentPacket.seqno + ", src=" + src + ", bytesCurBlock="
            + bytesCurBlock + ", fileSize=" + fileSize + ", appendChunk="
            + appendChunk);
      }
      /*
       * DFSClient.LOG.info("DFSClient writeChunk packet full seqno=" +
       * currentPacket.seqno + ", src=" + src + ", bytesCurBlock=" +
       * bytesCurBlock + ", fileSize=" + fileSize + ", appendChunk=" +
       * appendChunk);
       */
      waitAndCodeCurrentPacket();

      // If the reopened file did not end at chunk boundary and the above
      // write filled up its partial chunk. Tell the summer to generate full
      // crc chunks from now on.
      // if (appendChunk && bytesCurBlock%bytesPerChecksum == 0) {
      // appendChunk = false;
      // resetChecksumChunk(bytesPerChecksum);
      // }
      //
      // if (!appendChunk) {
      // int psize = Math.min((int)(blockSize-bytesCurBlock),
      // dfsClient.writePacketSize);
      // computePacketChunkSize(psize, bytesPerChecksum);
      // }
      //
      // if encountering a block boundary, send an empty packet to
      // indicate the end of block and reset bytesCurBlock.
      //
      if (bytesCurBlock == fileSize) {

        currentPacket = new Packet(PacketHeader.PKT_HEADER_LEN, 0,
            bytesCurBlock);
        currentPacket.lastPacketInBlock = true;
        bytesCurBlock = 0;
        lastFlushOffset = 0;
        // DFSClient.LOG.info("byteCurBlock == fileSize! and send a last packet in writechunk.");
        waitAndCodeCurrentPacket();

      }
    }
  }

  @Override
  @Deprecated
  public synchronized void sync() throws IOException {
    hflush();
  }

  /**
   * flushes out to all replicas of the block. The data is in the buffers of the
   * DNs but not neccessary on the DN's OS buffers.
   * 
   * It is a synchronous operation. When it returns, it guarantees that flushed
   * data become visible to new readers. It is not guaranteed that data has been
   * flushed to persistent store on the datanode. Block allocations are
   * persisted on namenode.
   */
  @Override
  public void hflush() throws IOException {
    dfsClient.checkOpen();
    isClosed();
    try {
      long toWaitFor;
      synchronized (this) {
        /*
         * Record current blockOffset. This might be changed inside
         * flushBuffer() where a partial checksum chunk might be flushed. After
         * the flush, reset the bytesCurBlock back to its previous value, any
         * partial checksum chunk will be sent now and in next packet.
         */
        long saveOffset = bytesCurBlock;
        Packet oldCurrentPacket = currentPacket;
        // flush checksum buffer, but keep checksum buffer intact
        flushBuffer(true);
        // bytesCurBlock potentially incremented if there was buffered data

        if (DFSClient.LOG.isDebugEnabled()) {
          DFSClient.LOG.debug("DFSClient flush() : saveOffset " + saveOffset
              + " bytesCurBlock " + bytesCurBlock + " lastFlushOffset "
              + lastFlushOffset);
        }
        // Flush only if we haven't already flushed till this offset.
        if (lastFlushOffset != bytesCurBlock) {
          assert bytesCurBlock > lastFlushOffset;
          // record the valid offset of this flush
          lastFlushOffset = bytesCurBlock;
          // waitAndCodeCurrentPacket();
          // waitAndQueueCurrentPacket();
        } else {
          // We already flushed up to this offset.
          // This means that we haven't written anything since the last flush
          // (or the beginning of the file). Hence, we should not have any
          // packet queued prior to this call, since the last flush set
          // currentPacket = null.
          assert oldCurrentPacket == null : "Empty flush should not occur with a currentPacket";

          // just discard the current packet since it is already been sent.
          currentPacket = null;
        }
        // Restore state of stream. Record the last flush offset
        // of the last full chunk that was flushed.
        //
        bytesCurBlock = saveOffset;
        toWaitFor = lastQueuedSeqno;
      } // end synchronized

      waitForAckedSeqno(toWaitFor);

      // If any new blocks were allocated since the last flush,
      // then persist block locations on namenode.
      //
      if (persistBlocks.getAndSet(false)) {
        try {
          dfsClient.namenode.fsync(src, dfsClient.clientName);
        } catch (IOException ioe) {
          DFSClient.LOG.warn("Unable to persist blocks in hflush for " + src,
              ioe);
          // If we got an error here, it might be because some other thread
          // called
          // close before our hflush completed. In that case, we should throw an
          // exception that the stream is closed.
          isClosed();
          // If we aren't closed but failed to sync, we should expose that to
          // the
          // caller.
          throw ioe;
        }
      }
    } catch (InterruptedIOException interrupt) {
      // This kind of error doesn't mean that the stream itself is broken - just
      // the
      // flushing thread got interrupted. So, we shouldn't close down the
      // writer,
      // but instead just propagate the error
      throw interrupt;
    } catch (IOException e) {
      DFSClient.LOG.warn("Error while syncing", e);
      synchronized (this) {
        if (!closed) {
          lastException = new IOException("IOException flush:" + e);
          closeThreads(true);
        }
      }
      throw e;
    }
  }

  /**
   * The expected semantics is all data have flushed out to all replicas and all
   * replicas have done posix fsync equivalent - ie the OS has flushed it to the
   * disk device (but the disk may have it in its cache).
   * 
   * Right now by default it is implemented as hflush
   */
  @Override
  public synchronized void hsync() throws IOException {
    hflush();
  }

  /**
   * Returns the number of replicas of current block. This can be different from
   * the designated replication factor of the file because the NameNode does not
   * replicate the block to which a client is currently writing to. The client
   * continues to write to a block even if a few datanodes in the write pipeline
   * have failed.
   * 
   * @return the number of valid replicas of the current block
   */
  public synchronized int getNumCurrentReplicas() throws IOException {
    dfsClient.checkOpen();
    isClosed();
    if (streamer == null) {
      return blockReplication; // no pipeline, return repl factor of file
    }
    DatanodeInfo[] currentNodes = streamer.getNodes();
    if (currentNodes == null) {
      return blockReplication; // no pipeline, return repl factor of file
    }
    return currentNodes.length;
  }

  /**
   * Waits till all existing data is flushed and confirmations received from
   * datanodes.
   */
  private void flushInternal() throws IOException {
    long toWaitFor;
    synchronized (this) {
      dfsClient.checkOpen();
      isClosed();
      //
      // If there is data in the current buffer, send it across
      //
      // queueCurrentPacket();
      // codeCurrentPacket();
      toWaitFor = lastQueuedSeqno;
    }

    waitForAckedSeqno(toWaitFor);
  }

  private void waitForAckedSeqno(long seqno) throws IOException {
    if (DFSClient.LOG.isDebugEnabled()) {
      DFSClient.LOG.debug("Waiting for ack for: " + seqno);
    }
    synchronized (dataQueue) {
      while (!closed) {
        isClosed();
        if (lastAckedSeqno >= seqno) {
          break;
        }
        try {
          dataQueue.wait(1000); // when we receive an ack, we notify on
                                // dataQueue
        } catch (InterruptedException ie) {
          throw new InterruptedIOException(
              "Interrupted while waiting for data to be acknowledged by pipeline");
        }
      }
    }
    isClosed();
  }

  /**
   * Aborts this output stream and releases any system resources associated with
   * this stream.
   */
  synchronized void abort() throws IOException {
    if (closed) {
      return;
    }
    streamer.setLastException(new IOException("Lease timeout of "
        + (dfsClient.hdfsTimeout / 1000) + " seconds expired."));
    closeThreads(true);
  }

  // shutdown datastreamer and responseprocessor threads.
  // interrupt datastreamer if force is true
  private void closeThreads(boolean force) throws IOException {
    try {
      streamer.close(force);
      streamer.join();
      if (s != null) {
        for (Socket sock : s)
          sock.close();
      }
    } catch (InterruptedException e) {
      throw new IOException("Failed to shutdown streamer");
    } finally {
      streamer = null;
      s = null;
      closed = true;
    }
  }

  /**
   * Closes this output stream and releases any system resources associated with
   * this stream.
   */
  @Override
  public synchronized void close() throws IOException {
    if (closed) {
      IOException e = lastException;
      if (e == null)
        return;
      else
        throw e;
    }

    try {
      flushBuffer(); // flush from all upper layers

      if (currentPacket != null) {
        // waitAndQueueCurrentPacket();
        // waitAndCodeCurrentPacket();
      }

      if (bytesCurBlock != 0) {
        // send an empty packet to mark the end of the block
        currentPacket = new Packet(PacketHeader.PKT_HEADER_LEN, 0,
            bytesCurBlock);
        currentPacket.lastPacketInBlock = true;
      }

      flushInternal(); // flush all data to Datanodes
      // get last block before destroying the streamer
      // TODO:
      // Block lastBlock = streamer.getBlock();
      closeThreads(false);
      DFSClient.LOG.info("Cumulus write time consumption:  "
          + (new Date().getTime() - DFSClient.startTime) + " ms");
      completeFile(src);
      // DFSClient.LOG.info("after compltefile: cumulus"+ (new
      // Date().getTime()-DFSClient.startTime));
      dfsClient.leasechecker.remove(src);
      pool.shutdown();
    } finally {
      closed = true;
    }
  }

  // should be called holding (this) lock since setTestFilename() may
  // be called during unit tests
  private void completeFile(String src) throws IOException {
    long localstart = System.currentTimeMillis();
    boolean fileComplete = false;
    for (Block b : blocks) {
      // DFSClient.LOG.info("Size of " + b.getBlockId() + " = " +
      // b.getNumBytes());
    }

    while (!fileComplete) {
      fileComplete = dfsClient.namenode.complete(src, dfsClient.clientName,
          blocks);
      if (!fileComplete) {
        if (!dfsClient.clientRunning
            || (dfsClient.hdfsTimeout > 0 && localstart + dfsClient.hdfsTimeout < System
                .currentTimeMillis())) {
          String msg = "Unable to close file because dfsclient "
              + " was unable to contact the HDFS servers." + " clientRunning "
              + dfsClient.clientRunning + " hdfsTimeout "
              + dfsClient.hdfsTimeout;
          DFSClient.LOG.info(msg);
          throw new IOException(msg);
        }
        try {
          Thread.sleep(400);
          if (System.currentTimeMillis() - localstart > 5000) {
            DFSClient.LOG.info("Could not complete file " + src
                + " retrying...");
          }
        } catch (InterruptedException ie) {
        }
      }
    }
  }

  void setArtificialSlowdown(long period) {
    artificialSlowdown = period;
  }

  synchronized void setChunksPerPacket(int value) {
    chunksPerPacket = Math.min(chunksPerPacket, value);
    packetSize = PacketHeader.PKT_HEADER_LEN
        + (checksum.getBytesPerChecksum() + checksum.getChecksumSize())
        * chunksPerPacket;
  }

  synchronized void setTestFilename(String newname) {
    src = newname;
  }

  /**
   * Returns the size of a file as it was when this stream was opened
   */
  long getInitialLen() {
    return initialFileSize;
  }

  /**
   * Returns the access token currently used by streamer, for testing only
   */
  Token<BlockTokenIdentifier> getBlockToken() {
    return streamer.getBlockToken();
  }

  class generateCheckSum extends Thread {
    int index;
    boolean last;

    public generateCheckSum(int index, boolean last) {
      // TODO Auto-generated constructor stub
      this.index = index;
      this.last = last;
    }

    public void run() {
      codeBuf[index].generateCheckSum();
      synchronized (dataQueue.get(index)) {
        dataQueue.get(index).addLast(codeBuf[index]);
        dataQueue.get(index).notify();
      }

      if (last) {
        Packet cPacket = new Packet(PacketHeader.PKT_HEADER_LEN, 0,
            bytesCurBlock);
        cPacket.lastPacketInBlock = true;

        cPacket.setOffset(offsetInB + codeBuf[index].dataPos
            - codeBuf[index].dataStart);

        synchronized (dataQueue.get(index)) {
          dataQueue.get(index).add(cPacket);
          dataQueue.get(index).notify();

        }
      }

      codeBuf[index] = null;

      synchronized (waitCheckSum) {
        chckSumCount--;
        waitCheckSum.notifyAll();
      }

      return;
    }
  }

  class mult extends Thread {

    byte element;
    Packet packet;

    mult(Packet packet, byte element) {
      this.element = element;
      this.packet = packet;
    }

    @Override
    public void run() {
      // TODO Auto-generated method stub
      packet.mult(element);
      synchronized (object) {
        count--;
        object.notifyAll();
      }
      ;
      return;
    }

  }

  class code extends Thread {
    Packet p1;
    Packet p2;
    byte element;

    code(Packet p1, Packet p2, byte element) {
      this.p1 = p1;
      this.p2 = p2;
      this.element = element;
    }

    @Override
    public void run() {
      // TODO Auto-generated method stub
      p1.code(p2, element);
      synchronized (object) {
        count--;
        object.notifyAll();
      }
      return;
    }

  }
}
