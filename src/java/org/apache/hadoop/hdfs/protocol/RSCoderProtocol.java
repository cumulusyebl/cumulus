package org.apache.hadoop.hdfs.protocol;






public class RSCoderProtocol{
	
	  private static int NW = (1 << 8);
	  private static short[] gflog = new short[NW];;
	  private static short[] gfilog = new short[NW];;
	  public static short[][] div = new short[NW][NW];
	  public static short[][] mult = new short[NW][NW];
	  
	  private static RSCoderProtocol rsp = new RSCoderProtocol();
	 
	  
  public static RSCoderProtocol getRSP(){
	  return rsp;
  }
  
  private RSCoderProtocol(){
	  setup_tables();
	  CalculateValue();
  }
    
  private void setup_tables(){        
        int b = 1;
        for (int log = 0; log < NW - 1; log++)
        {
            gflog[b] = (short)log;
            gfilog[log] = (short)b;
            b = (b << 1);
            if ((b & (0x0100)) != 0) b = (b ^ (0x011D));
        }
    }
	    
  //GF multiply
 private short multV(int a, int b){
        int sum_log;
        if (a == 0 || b == 0) return 0;
        sum_log = gflog[a] + gflog[b];
        if (sum_log >= (NW - 1)) sum_log -= (NW - 1);
        return gfilog[sum_log];
 }
	    
//GF divide
 private short divV(int a, int b){
        int diff_log;
        if (a == 0) return 0;
        if (b == 0) return 0;
        diff_log = gflog[a] - gflog[b];
        if (diff_log < 0) diff_log += NW - 1;
        return gfilog[diff_log];
 }
	    
 private void CalculateValue(){
    
        for(int i=0;i<NW;i++)
	        for (int j = 0; j < NW; j++){                 	
				   mult[i][j] = multV(i,j);
	             div[i][j] = divV(i,j);               
	         }
 }
 public byte[][] InitialCauchyMatrix(int k, int n){            
    	 byte[][] G=new byte[k][n];
    	 short[][] E = new short[k][];
	      
        for (int i = 0; i < k; i++){
            E[i] = new short[n];
         
            for (int j = 0; j < k; j++)
                if (i == j) E[i][j] = 1;
                else E[i][j] = 0;
         }
        for (short j = 0; j < n; j++)
            for (short i = 0; i < k; i++)
                E[i][j] = div[1][(j) ^ (i + n)];
	     
       
        for (short j = 0; j < n; j++)
            for (short i = 0; i < k; i++)
                G[i][j]=(byte) E[i][j];
        return G;
}  
	    
 
	    
private void swap(int j, short[][] g, short[][] E){
    short max = g[j][j];
    int i = -1;
    for (int k = j + 1; k < g.length ; k++){
        if (g[k][j] > max)
        {
            i = k;
            max = g[k][j];
        }
    }
    if (i != -1){
      	short[] temp;
        temp = E[j];
        E[j] = E[i];
        E[i] = temp;
        temp = g[j];
        g[j] = g[i];
        g[i] = temp;
    }
}
	    
public short[][] InitialInvertedCauchyMatrix(short[][]g){   
    short[][] E;
    E = new short[g.length][g.length];
  
    for (int i = 0; i < g.length; i++) {
        for (int j = 0; j < g.length; j++)
            if (i == j) E[i][j] = 1;
            else E[i][j] = 0;
       }
 
    for (int i = 0; i < g.length; i++){
        swap(i,g,E);
        int k = g[i][i];
        if (k > 1)
        {
            for (int j = 0; j < g.length; j++)
            {
               g[i][j] = div[g[i][j]][k];
               E[i][j] = div[E[i][j]][k];
            }
        }
        for (int j = 0; j < g.length; j++)
        {
            if ((j == i) || (g[j][i] == 0)) continue;
            k = g[j][i];
            for (int t = 0; t < g.length; t++)
            {
                g[j][t] = div[g[j][t]][ k];
                g[j][t] ^= g[i][t];
                E[j][t] = div[E[j][t]][k];
                E[j][t] ^= E[i][t];
            }
        }
    }
    for (int i = 0; i < g.length; i++){
        if ((g[i][i] != 1))
            for (int j = 0; j < g.length; j++)
                E[i][j] = div[E[i][j]][g[i][i]];
    }
	return E;

}
	    
	    
}
