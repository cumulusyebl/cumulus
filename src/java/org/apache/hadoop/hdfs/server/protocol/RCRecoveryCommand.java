package org.apache.hadoop.hdfs.server.protocol;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.protocol.CodingMatrix;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;
import org.apache.hadoop.security.token.Token;

// seq RCR_NN_APPOINTnc.1 1
/**
 * This is the command to guide a datanode to recover a group of blocks in RCR
 * and is based on CumulusRecoveryCommand. created at 2014-4-21. modified at
 * 
 * @author ds
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class RCRecoveryCommand extends DatanodeCommand
{
	byte failednodeRow;
	LocatedBlock[] fileLocatedBlocks;
	CodingMatrix matrix;
	byte type;

	public RCRecoveryCommand()
	{
	}

	public RCRecoveryCommand(int action, byte type, byte failednodeRow,
			CodingMatrix matrix, LocatedBlock[] fileLocatedBlocks)
	{
		super(action);
		this.type = type;
		this.failednodeRow = failednodeRow;
		this.matrix = matrix;
		this.fileLocatedBlocks = fileLocatedBlocks;
	}

	public LocatedBlock getLocatedBlk(int i)
	{
		return fileLocatedBlocks[i];
	}

	public LocatedBlock[] getLocatedBlks()
	{
		return fileLocatedBlocks;
	}

	public void setBlockToken(int i, Token<BlockTokenIdentifier> token)
	{
		fileLocatedBlocks[i].setBlockToken(token);
	}

	public int getSize()
	{
		return fileLocatedBlocks.length;
	}

	public CodingMatrix getMatrix()
	{
		return matrix;
	}

	public byte getLostRow()
	{
		return failednodeRow;
	}

	public static boolean isRCRecovery()
	{
		return true;
	}

	// /////////////////////////////////////////
	// Writable
	// /////////////////////////////////////////
	static
	{ // register a ctor
		WritableFactories.setFactory(RCRecoveryCommand.class,
				new WritableFactory()
				{
					public Writable newInstance()
					{
						return new RCRecoveryCommand();
					}
				});
	}

	public void write(DataOutput out) throws IOException
	{
		super.write(out);
		out.write(failednodeRow);
		out.writeInt(fileLocatedBlocks.length);
		for (int i = 0; i < fileLocatedBlocks.length; i++)
		{
			fileLocatedBlocks[i].write(out);
		}
		out.write(type);
		matrix.write(out);
	}

	public void readFields(DataInput in) throws IOException
	{
		super.readFields(in);
		this.failednodeRow = in.readByte();
		this.fileLocatedBlocks = new LocatedBlock[in.readInt()];
		for (int i = 0; i < fileLocatedBlocks.length; i++)
		{
			fileLocatedBlocks[i] = new LocatedBlock();
			fileLocatedBlocks[i].readFields(in);
		}
		byte type = in.readByte();
		matrix = CodingMatrix.getMatrixofCertainType(type);
		matrix.readFields(in);
	}

}
