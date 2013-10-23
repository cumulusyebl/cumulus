package org.apache.hadoop.hdfs.protocol;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.hdfs.protocol.RSCoderProtocol;

import java.util.List;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;
//TODO:
public class CodingMatrix implements Writable{
	byte[][] matrix;
	byte row;
	byte column;
	public CodingMatrix(){
		row = 0;
		column = 0;
		matrix = null;
	}
	public CodingMatrix(long fileLength){
		RSCoderProtocol rsp = RSCoderProtocol.getRSP();	
		row = 3;
		column = 4;
		matrix=(byte[][])rsp.InitialCauchyMatrix(row, column);
//		row = 3;
//		column = 4;
//		matrix = new byte[row][column];
//		matrix[0][0] = 1;
//		matrix[0][1] = 0;
//		matrix[0][2] = 0;
//		matrix[0][3] = 1;
//		matrix[1][0] = 1;
//		matrix[1][1] = 1;
//		matrix[1][2] = 1;
//		matrix[1][3] = 1;
//		matrix[2][0] = 0;
//		matrix[2][1] = 1;
//		matrix[2][2] = 0;
//		matrix[2][3] = 1;
	}
	
	public CodingMatrix(CodingMatrix matrix) {
		this.row = matrix.row;
		this.column = matrix.column;
		this.matrix = new byte[row][column];
		for (int i = 0; i < row; i++) {
			for (int j = 0; j < column; j++) {
				this.matrix[i][j] = matrix.getElemAt(i, j); 
			}
		}
	}
	
	public CodingMatrix(byte row, byte column) {
		this.row = row;
		this.column = column;
		matrix = new byte[row][column];
	}
	public byte[][] getCodingmatrix(){
		 return matrix;
	}
	public byte getRow(){
		return row;
	}
	public byte getColumn(){
		return column;
	}
	
//	public List<byte> getRowList(int row){
//		List<byte> s = new ArrayList<byte>();
//		for(int i = 0; i < column; i++)
//			s.add((matrix[row][i]));
//		return s;
//	}
	
	public byte[] getRowArray(int row){
		byte[] s = new byte[column];
		for(int i = 0; i < column; i++)
			s[i] = matrix[row][i];
		return s;
	}
	public int[] getCodingFactorList(int row){
//		int[] cf = new int[column];
		List<Integer> list = new ArrayList<Integer>();
		for(int i = 0; i < column; i++){
			if(matrix[row][i] != 0){
				int cf = ((int)matrix[row][i] << 16) + getLastID(i);
				list.add(cf);
			}
		}
		int[] cfl = new int[list.size()];
		for (int i = 0; i < list.size(); i++) {
			cfl[i] = list.get(i);
		}
		return cfl;
	}
	public int getLastID(int column){
		int id = 0;
		for(int i = 0; i < row; i++){
			if(matrix[i][column] != 0){
				id += (1 << i);
			}
		}
		return id;
	}

//	public List<byte[]> Matrix2Serializetion(){
//		List m2sList = new ArrayList<byte[]>();
//		byte[] b = new byte[row];
//		for (int i = 0; i < column; i++) {
//			for (int j = 0; j < row; j++) {
//				b[j] = Matrixat(i ,j);
//			}
//			m2sList.add(b);
//		}
//		
//		return m2sList;
//	}
	public String toString(){
		String s = "Row: " + row + " Column: " + column;
		 for (int i = 0; i < row; i++) {
				s += "\n";
			for (int j = 0; j < column; j++) {
				s += "  " + matrix[i][j];
			}
		}
		return s;
	}
	public byte getElemAt(int i, int j) {
		return matrix[i][j];
	}
	public void setElemAt(int i, int j, byte b) {
		this.matrix[i][j] = b;
	}
	
 ///////////////////////////////////////////
  // Writable
  ///////////////////////////////////////////
  static {                                      // register a ctor
    WritableFactories.setFactory
      (CodingMatrix.class,
       new WritableFactory() {
         public Writable newInstance() { return new CodingMatrix(); }
       });
  }
	  
	@Override
	public void readFields(DataInput input) throws IOException {
		this.row = input.readByte();
		this.column = input.readByte();
		this.matrix = new byte[row][column];
		for(byte i = 0; i < row; ++i)
			for(int j = 0; j < column; ++j){
				matrix[i][j] = input.readByte();
			}
	}
	@Override
	public void write(DataOutput output) throws IOException {
		output.writeByte(row);
		output.writeByte(column);
		for(byte i = 0; i < row; ++i)
			for(int j = 0; j < column; ++j){
				output.writeByte(matrix[i][j]);
			}
		
	}
}