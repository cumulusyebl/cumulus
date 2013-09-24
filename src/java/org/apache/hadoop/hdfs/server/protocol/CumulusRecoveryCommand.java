package org.apache.hadoop.hdfs.server.protocol;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.CodingMatrix;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.DatanodeDescriptor.BlockTargetPair;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;


/**
 * command to guide datanode recoverying block
 * @author czl
 *
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class CumulusRecoveryCommand extends DatanodeCommand{
	Block blocks[];
	DatanodeInfo targets[][];
	CodingMatrix matrix;
	
	
	public CumulusRecoveryCommand() {	}
	
	
	public CumulusRecoveryCommand(int action, CodingMatrix matrix, List<BlockTargetPair> blockTargetPairs){
		super(action);
		blocks = new Block[blockTargetPairs.size()]; 
	    targets = new DatanodeInfo[blocks.length][];
	    for(int i = 0; i < blocks.length; i++) {
	      BlockTargetPair p = blockTargetPairs.get(i);
	      blocks[i] = p.block;
	      targets[i] = p.targets;
	    }
	    this.matrix = matrix;
	}

	public Block[] getBlocks() {
		return blocks;
	}

	public DatanodeInfo[][] getTargets() {
		return targets;
	}
	
	public CodingMatrix getMatrix() {
		return matrix;
	}
	

	  ///////////////////////////////////////////
	  // Writable
	  ///////////////////////////////////////////
	  static {                                      // register a ctor
	    WritableFactories.setFactory
	      (CumulusRecoveryCommand.class,
	       new WritableFactory() {
	         public Writable newInstance() { return new CumulusRecoveryCommand(); }
	       });
	  }

	  public void write(DataOutput out) throws IOException {
	    super.write(out);
	    out.writeInt(blocks.length);
	    for (int i = 0; i < blocks.length; i++) {
	      blocks[i].write(out);
	    }
	    out.writeInt(targets.length);
	    for (int i = 0; i < targets.length; i++) {
	      out.writeInt(targets[i].length);
	      for (int j = 0; j < targets[i].length; j++) {
	        targets[i][j].write(out);
	      }
	    }
	    matrix.write(out);
	  }

	  public void readFields(DataInput in) throws IOException {
	    super.readFields(in);
	    this.blocks = new Block[in.readInt()];
	    for (int i = 0; i < blocks.length; i++) {
	      blocks[i] = new Block();
	      blocks[i].readFields(in);
	    }

	    this.targets = new DatanodeInfo[in.readInt()][];
	    for (int i = 0; i < targets.length; i++) {
	      this.targets[i] = new DatanodeInfo[in.readInt()];
	      for (int j = 0; j < targets[i].length; j++) {
	        targets[i][j] = new DatanodeInfo();
	        targets[i][j].readFields(in);
	      }
	    }
	    matrix = new CodingMatrix();
	    matrix.readFields(in);
	  }

}
