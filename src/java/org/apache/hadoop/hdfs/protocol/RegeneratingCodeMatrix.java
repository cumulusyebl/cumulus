package org.apache.hadoop.hdfs.protocol;

//seq RC.2 1
/***
 * This class is a copy of XORCoderPrptocol, to carry the regenerating code matrix. created at 2014-4-12. modified at
 * 2014-4-23
 * 
 * @author ds
 */
public class RegeneratingCodeMatrix extends CodingMatrix
{
	// int storeFileNodesNum; // n
	// int recoveryMinNodesNum; // k
	// int recoveryNodesNum; // d
	// int perNodeBlocksNum; // alfa, also a
	// int fileCutsNum; // B
	int n = 6;
	int k = 3;
	int d = 4;

	public RegeneratingCodeMatrix()
	{

		row = 9;
		column = 24;
		matrix = new byte[row][column];
		// get message matrix : M
		byte[][] messageMatrix =
		{
		{ 1, 2, 3, 7 },
		{ 2, 4, 5, 8 },
		{ 3, 5, 6, 9 },
		{ 7, 8, 9, 0 } };
		// get Vandeemonde matrix : V
		byte[][] vandermondeMatrix = getVandermondeMatrix();

		// compute M and V, result is coding matrix
		for (byte row = 0; row < 6; row++)
		{
			for (byte column = 0; column < 4; column++)
			{
				for (byte index = 0; index < 4; index++)
				{
					if (messageMatrix[index][column] - 1 >= 0)
					{
						matrix[messageMatrix[index][column] - 1][column + row * 4] = vandermondeMatrix[row][index];
					}

				}
			}
		}
	}

	public RegeneratingCodeMatrix(long fileLength)
	{

		this();
	}

	public RegeneratingCodeMatrix(byte row, byte column)
	{
		this.row = row;
		this.column = column;
		this.matrix = new byte[row][column];
	}

	@Override
	public byte code(byte b1, byte b2, byte element)
	{
		short Buf, inputByte, key;

		if (b2 < 0)
			inputByte = (short) (b2 + 256);
		else
			inputByte = (short) b2;

		if (b1 < 0)
			Buf = (short) (b1 + 256);
		else
			Buf = (short) b1;

		if (element < 0)
			key = (short) (element + 256);
		else
			key = (short) (element);

		Buf ^= RSCoderProtocol.mult[inputByte][key];

		return (byte) Buf;
	}

	@Override
	public byte mult(byte b1, byte element)
	{
		short b, key;

		if (b1 < 0)
			b = (short) (b1 + 256);
		else
			b = (short) b1;

		if (element < 0)
			key = (short) (element + 256);
		else
			key = (short) element;
		b = RSCoderProtocol.mult[b][key];

		return (byte) b;
	}

	@Override
	public int decoder(short[][] VdcMatrixInShort, byte[][] rawBuf, int[] buflen, int offset, byte[] resultBuf)
	{
		//long begin = System.currentTimeMillis();

		// a box :h, w, l
		int k_nodes = VdcMatrixInShort.length;
		int d = VdcMatrixInShort[0].length;
		int indexCount = rawBuf[0].length;
		// get Phi_DC_Inverse_Matrix
		short[][] Pdc = new short[k_nodes][k_nodes];
		for (int i = 0; i < Pdc.length; i++)
		{
			for (int j = 0; j < Pdc[i].length; j++)
			{
				Pdc[i][j] = VdcMatrixInShort[i][j];
			}
		}
		short[][] PdcInverse = RSCoderProtocol.getRSP().InitialInvertedCauchyMatrix(Pdc);
		// get Delta_DC_Matrix
		short[][] Ddc = new short[k_nodes][d - k_nodes];
		for (int i = 0; i < Ddc.length; i++)
		{
			for (int j = 0; j < Ddc[i].length; j++)
			{
				Ddc[i][j] = VdcMatrixInShort[i][k_nodes + j];
			}
		}

		// PDdc = PdcInverse * Ddc
		short[][] PDdc = matrixMult(PdcInverse, Ddc);

		//long getSomeMatrixEnd = System.currentTimeMillis();
		//DFSClient.LOG.info("getSomeMatrix :" + (getSomeMatrixEnd - begin));

		// change decodebuf to 3D array
		short[][][] box = new short[indexCount][k_nodes][d];
		for (int index = 0; index < indexCount; index++)
		{
			for (int row = 0; row < k_nodes; row++)
			{
				int num = 0;
				byte tmp = 0;
				for (int column = 0; column < d; column++)
				{
					num = row * d + column;
					tmp = rawBuf[num][index];
					box[index][row][column] = tmp >= 0 ? (short) tmp : (short) (tmp + 256);
				}
			}
		}
		//long bufToBoxEnd = System.currentTimeMillis();
		//DFSClient.LOG.info("bufToBox:" + (bufToBoxEnd - getSomeMatrixEnd));

		//
		// /
		// short[][][] resultBox = new short[indexCount][k_nodes][d];
		for (int index = 0; index < indexCount; index++)
		{
			// PdcInverse left mult todecodeBox[index]
			// //resultBox[index] = matrixMult(PdcInverse, toDecodeBox[index]);
			int rowNum = PdcInverse.length;
			int columnNum = box[index][0].length;
			int kNum = Pdc[0].length;
			for (int i = 0; i < rowNum; i++)
			{
				for (int j = 0; j < columnNum; j++)
				{
					short temp = 0;
					short f1 = 0;
					short f2 = 0;
					for (int k = 0; k < kNum; k++)
					{
						f1 = PdcInverse[i][k];
						f2 = box[index][k][j];
						temp ^= RSCoderProtocol.mult[f1][f2];
					}
					box[index][i][j] = temp;
				}
			}
		}
		//long leftMultEnd = System.currentTimeMillis();
		//DFSClient.LOG.info("leftMult :" + (leftMultEnd - bufToBoxEnd));

		for (int index = 0; index < indexCount; index++)
		{
			// minus
			int rowNum = k_nodes;
			int columnNum = k_nodes;
			for (int i = 0; i < rowNum; i++)
			{
				for (int j = i; j < columnNum; j++)
				{
					short f1 = PdcInverse[i][0];
					short f2 = box[index][j][k_nodes + 0];
					box[index][i][j] ^= RSCoderProtocol.mult[f1][f2];
				}
			}
		}
		//long minusTEnd = System.currentTimeMillis();
		//DFSClient.LOG.info("minusT :" + (minusTEnd - leftMultEnd));

		//
		int pos = 0;

		// u0-u5
		for (int i = 0; i < k_nodes; i++)
		{
			for (int j = i; j < k_nodes; j++)
			{

				for (int index = 0; index < indexCount; index++)
				{
					resultBuf[pos] = (byte) box[index][i][j];
					pos++;
				}
			}
		}
		// u6-u8
		for (int i = 0; i < k_nodes; i++)
		{
			for (int index = 0; index < indexCount; index++)
			{

				resultBuf[pos] = (byte) box[index][i][d - 1];
				pos++;
			}
		}
		//long boxToBufEnd = System.currentTimeMillis();
		//DFSClient.LOG.info("boxToBuf:" + (boxToBufEnd - minusTEnd));
		return pos - offset;
	}

	private short[][] matrixMult(short[][] m1, short[][] m2)
	{
		if (m1 == null || m2 == null || m1[0].length != m2.length)
		{
			return null;
		}
		int rowNum = m1.length;
		int columnNum = m2[0].length;
		int kNum = m1[0].length;
		short[][] r = new short[rowNum][columnNum];
		for (int i = 0; i < rowNum; i++)
		{
			for (int j = 0; j < columnNum; j++)
			{
				for (int k = 0; k < kNum; k++)
				{
					short f1 = m1[i][k];
					short f2 = m2[k][j];
					r[i][j] ^= RSCoderProtocol.mult[f1][f2];
				}
			}
		}
		return r;
	}

	private short[][] matrixMinus(short[][] m1, short[][] m2)
	{
		if (m1 == null || m2 == null || m1[0].length < m2[0].length || m1.length < m2.length)
		{
			return null;
		}

		for (int i = 0; i < m2.length; i++)
		{
			for (int j = 0; j < m2[i].length; j++)
			{
				m1[i][j] = (short) (m1[i][j] ^ m2[i][j]);
			}
		}
		return m1;
	}

	private short[][] matrixTranspose(short[][] m)
	{
		if (m == null)
		{
			return null;
		}
		int rowNum = m[0].length;
		int columnNum = m.length;
		short[][] r = new short[rowNum][columnNum];

		for (int i = 0; i < r.length; i++)
		{
			for (int j = 0; j < r[i].length; j++)
			{
				r[i][j] = (short) (m[j][i]);
			}
		}
		return r;
	}

	@Override
	public int getStoreFileNodesNum()
	{
		int rowNum = 9;
		int columnNum = 24;
		byte[][] matrix = new byte[rowNum][columnNum];
		byte[][] messageMatrix =
		{
		{ 1, 2, 3, 7 },
		{ 2, 4, 5, 8 },
		{ 3, 5, 6, 9 },
		{ 7, 8, 9, 0 } };
		byte[][] vandermondeMatrix = getVandermondeMatrix();

		for (byte row = 0; row < 6; row++)
		{
			for (byte column = 0; column < 4; column++)
			{
				for (byte index = 0; index < 4; index++)
				{
					if (messageMatrix[index][column] - 1 >= 0)
					{
						matrix[messageMatrix[index][column] - 1][column * 6 + row] = vandermondeMatrix[row][index];
					}
				}
			}
		}

		int minN = columnNum;
		for (int i = 0; i < rowNum; i++)
		{
			int n = 0;
			for (int j = 0; j < columnNum; j++)
			{

				if (matrix[i][j] != 0)
				{
					n++;
				}
				else
				{
					break;
				}
			}
			if (n != 0 && n < minN)
			{
				minN = n;
			}
		}
		return minN;
	};

	@Override
	public int getPerNodeBlocksNum()
	{
		return column / getStoreFileNodesNum();
	};

	@Override
	public int getRecoveryMinNodesNum()
	{
		return k;
	};

	@Override
	public int getRecoveryNodesNum()
	{
		return getPerNodeBlocksNum();
	};

	@Override
	public int getFileCutsNum()
	{
		return row;
	};

	@Override
	public byte[][] getVandermondeMatrix()
	{
		byte[][] vandermondeMatrix =
		{
		{ 1, 1, 1, 1 },
		{ 1, 2, 4, 1 },
		{ 1, 3, 2, 6 },
		{ 1, 4, 2, 1 },
		{ 1, 5, 4, 6 },
		{ 1, 6, 1, 6 } };

		// compute V
		for (byte row = 0; row < vandermondeMatrix.length; row++)
		{
			for (byte column = 2; column <= 3; column++)
			{
				byte tmp = 1;
				for (byte k = 0; k < column; k++)
				{
					tmp = RSCoderProtocol.getRSP().mult(tmp, (byte) (row + 1));
				}
				vandermondeMatrix[row][column] = tmp;
			}
		}
		return vandermondeMatrix;
	};

	@Override
	public int getN()
	{
		return getStoreFileNodesNum();
	};

	@Override
	public int getA()
	{
		return getPerNodeBlocksNum();
	};

	@Override
	public int getK()
	{
		return getRecoveryMinNodesNum();
	};

	@Override
	public int getD()
	{
		return getRecoveryNodesNum();
	};

	@Override
	public int getB()
	{
		return getFileCutsNum();
	};

	// use RCR or not
	public static boolean isRegeneratingCodeRecovery()
	{
		return true;
	}

	public static String getMatrixLog(short[][] matrix, String name)
	{
		StringBuffer sb = new StringBuffer();
		sb.append(name).append("------dsLog").append('\n');
		for (int row = 0; row < matrix.length; row++)
		{
			for (int column = 0; column < matrix[row].length; column++)
			{
				sb.append(matrix[row][column]).append(' ');
			}
			sb.append('\n');
		}
		return sb.toString();
	}

}
