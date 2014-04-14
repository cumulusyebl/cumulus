package org.apache.hadoop.hdfs.server.protocol;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.protocol.CodingMatrix;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.server.namenode.DatanodeDescriptor.BlockTargetPair;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;
import org.apache.hadoop.security.token.Token;


/**
 * command to guide datanode recoverying block
 * @author czl
 *
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class CumulusRecoveryCommand extends DatanodeCommand{
	LocatedBlock[] locatedblks;
	CodingMatrix matrix;
	byte type;
	byte lostColumn;
	
	
	public CumulusRecoveryCommand() {	}
	
	
	public CumulusRecoveryCommand(int action, byte type, byte lostColumn, CodingMatrix matrix, List<BlockTargetPair> blockTargetPairs){
		super(action);
		this.type = type;
		this.lostColumn = lostColumn;
		locatedblks = new LocatedBlock[blockTargetPairs.size()];
		for(int i = 0; i < locatedblks.length; i++) {
	      BlockTargetPair p = blockTargetPairs.get(i);
	      locatedblks[i]= new LocatedBlock(p.block, p.targets); 
	    }
	    this.matrix = matrix;
	}
	
	public LocatedBlock getLocatedBlk(int i){
		return locatedblks[i];
	}
	
	public LocatedBlock[] getLocatedBlks() {
		return locatedblks;
	}
	
	public void setBlockToken(int i, Token<BlockTokenIdentifier> token) {
		locatedblks[i].setBlockToken(token);
	}
	
	public int getSize(){
		return locatedblks.length;
	}

	
	public CodingMatrix getMatrix() {
		return matrix;
	}
	
	public byte getLostColumn() {
		return lostColumn;
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
	    out.write(lostColumn);
	    out.writeInt(locatedblks.length);
	    for (int i = 0; i < locatedblks.length; i++) {
	      locatedblks[i].write(out);
	    }
	    out.write(type);
	    matrix.write(out);
	  }

	  public void readFields(DataInput in) throws IOException {
	    super.readFields(in);
	    this.lostColumn = in.readByte();
	    this.locatedblks = new LocatedBlock[in.readInt()];
	    for (int i = 0; i < locatedblks.length; i++) {
	      locatedblks[i] = new LocatedBlock();
	      locatedblks[i].readFields(in);
	    }
	    byte type = in.readByte();
	    matrix = CodingMatrix.getMatrixofCertainType(type);
	    matrix.readFields(in);
	  }

}
