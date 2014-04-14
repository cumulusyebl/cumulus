package org.apache.hadoop.hdfs.protocol;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.hdfs.DFSClient;


public class XORCoderProtocol extends CodingMatrix{
	
	public XORCoderProtocol(long fileLength){
		// RSCoderProtocol rsp = RSCoderProtocol.getRSP();	
		// row = 5;
		// column = 6;
		// matrix=(byte[][])rsp.InitialCauchyMatrix(row, column);
		row = 3;
		column = 4;
		matrix = new byte[row][column];
		matrix[0][0] = 1;
		matrix[0][1] = 0;
		matrix[0][2] = 0;
		matrix[0][3] = 1;
		matrix[1][0] = 1;
		matrix[1][1] = 1;
		matrix[1][2] = 1;
		matrix[1][3] = 1;
		matrix[2][0] = 0;
		matrix[2][1] = 1;
		matrix[2][2] = 0;
		matrix[2][3] = 1;
	}
	
	public XORCoderProtocol(byte row, byte column){
		this.row = row;
		this.column = column;
		this.matrix = new byte[row][column];
	}

	public XORCoderProtocol() {
		// TODO Auto-generated constructor stub
		row = 3;
		column = 4;
		matrix = new byte[row][column];
		matrix[0][0] = 1;
		matrix[0][1] = 0;
		matrix[0][2] = 0;
		matrix[0][3] = 1;
		matrix[1][0] = 1;
		matrix[1][1] = 1;
		matrix[1][2] = 1;
		matrix[1][3] = 1;
		matrix[2][0] = 0;
		matrix[2][1] = 1;
		matrix[2][2] = 0;
		matrix[2][3] = 1;
	}

	@Override
	public byte code(byte b1, byte b2, byte element) {
		
		// TODO Auto-generated method stub
		return (byte) (b1^b2);
	}

	@Override
	public byte mult(byte b1, byte element) {
		// TODO Auto-generated method stub
		return b1;
	}

	public int decoder(short[][] g,byte[][] Buf,int[] buflen,int offset,byte[] buf){
		    //g是编码矩阵，Buf是用于解码的数据，buflen对应Buf里数据的长度，解码得到的数据放到buf中从offset开始的位置
		    //返回的是buf中写入的长度
		    int len = 0;
		    int off = offset;
		    g = RSCoderProtocol.getRSP().InitialInvertedCauchyMatrix(g);
		    int k = g.length;
		    
		    for(int t = 0; t < k;t++)
		    {
		      len = 0;
		      for(int j = 0;j < k;j++)
		      {
		        if(g[j][t] != 0)
		        {
		          if(buflen[j] != -1 && len < buflen[j])
		            len = buflen[j];
		        }
		      }
		      short[] Output = new short[len];
		      Arrays.fill(Output,(short)0);
		      for(int i=0;i < k;i++)
		      {
		        if(g[i][t] != 0)
		        {
		          for(int j = 0;j < buflen[i];j++)
		            Output[j] = (short) (Output[j] ^ Buf[i][j]);
		        }
		      }

		      for(int i = 0 ;i < Output.length;i++)
		        buf[off++] = (byte)Output[i];
		    }

		    return (off - offset);
		  }
	
	

}
