
package org.apache.hadoop.hdfs.server.monitor;
/**
 * author: xianyu
 * date: 2014-04-07
 * */

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;

/**
 * ServernodeNETStatus represents the status of a net card.
 * net_rate means the network speed, in byte per second.
 * rx : download
 * tx : upload 
 * */
public class ServernodeNETStatus implements Serializable, Writable{
	private static final long serialVersionUID = 1L;
	
	private String cardname;
	private int bandwidth;
	private String duplex;
	private double cur_net_rx_rate, cur_net_tx_rate;//Bps, byte per second
	private double avg_net_rx_rate, avg_net_tx_rate;
	private double max_net_rx_rate, max_net_tx_rate;
	private double min_net_rx_rate, min_net_tx_rate;
	
	public ServernodeNETStatus(){
		this("null");
	}
	
	public ServernodeNETStatus(String cardname){
		String[] ss = cardname.split("/");
		this.cardname = ss[ss.length - 1];
		bandwidth = 0;
		duplex = "unknown";
		cur_net_rx_rate = cur_net_tx_rate = 0;
		avg_net_rx_rate = avg_net_tx_rate = 0;
		max_net_rx_rate = max_net_tx_rate = 0;
		min_net_rx_rate = min_net_tx_rate = 0;
	}
	
	public ServernodeNETStatus(ServernodeNETStatus that){
		this.cardname = that.cardname;
		this.bandwidth = that.bandwidth;
		this.duplex = that.duplex;
		this.cur_net_rx_rate = that.cur_net_rx_rate;
		this.cur_net_tx_rate = that.cur_net_tx_rate;
		this.avg_net_rx_rate = that.avg_net_rx_rate;
		this.avg_net_tx_rate = that.avg_net_tx_rate;
		this.max_net_rx_rate = that.max_net_rx_rate;
		this.max_net_tx_rate = that.max_net_tx_rate;
		this.min_net_rx_rate = that.min_net_rx_rate;
		this.min_net_tx_rate = that.min_net_tx_rate;
	}
	
	public String toString(){
		return String.format("[net status]\n" + 
			"\tcardname: %s; bandwidth: %d Mb; duplex: %s\n" + 
			"\tcur-rx-rate: %.0f Bps; cur-tx-rate: %.0f Bps\n" + 
			"\tavg-rx-rate: %.0f Bps; avg-tx-rate: %.0f Bps\n" + 
			"\tmax-rx-rate: %.0f Bps; max-tx-rate: %.0f Bps\n" + 
			"\tmin-rx-rate: %.0f Bps; min-tx-rate: %.0f Bps\n", 
				cardname, bandwidth, duplex, 
				cur_net_rx_rate, cur_net_tx_rate, 
				avg_net_rx_rate, avg_net_tx_rate, 
				max_net_rx_rate, max_net_tx_rate, 
				min_net_rx_rate, min_net_tx_rate);
	}
	
	public String toDump(){
		return String.format("%s %dMb %s %.0fBps %.0fBps %.0fBps %.0fBps" + 
			" %.0fBps %.0fBps %.0fBps %.0fBps", 
				cardname, bandwidth, duplex, 
				cur_net_rx_rate, cur_net_tx_rate, 
				avg_net_rx_rate, avg_net_tx_rate, 
				max_net_rx_rate, max_net_tx_rate, 
				min_net_rx_rate, min_net_tx_rate);
	}

	//to get the card name
	public String getCardname(){
		return cardname;
	}
	//to set the card name
	public void setCardname(String cardname){
		this.cardname = cardname;
	}
	
	//to get the bandwidth of network card
	public int getBandwidth(){
		return bandwidth;
	}
	//to set the bandwidth of network card
	public void setBandwidth(int bandwidth){
		this.bandwidth = bandwidth;
	}
	
	//to get the duplex of network card: full | half
	public String getDuplex(){
		return duplex;
	}
	//to set the duplex of network card: full | half
	public void setDuplex(String duplex){
		this.duplex = duplex;
	}
	
	//to get current net receive(download) rate
	public double getCurNETRxRate(){
		return cur_net_rx_rate;
	}
	//to set current net receive(download) rate
	public void setCurNETRxRate(double r){
		this.cur_net_rx_rate = r;
	}
	
	//to get current net transform(upload) rate
	public double getCurNETTxRate(){
		return cur_net_tx_rate;
	}
	//to get current net transform(upload) rate
	public void setCurNETTxRate(double r){
		this.cur_net_tx_rate = r;
	}
	
	//to get average net receive(download) rate
	public double getAvgNETRxRate(){
		return avg_net_rx_rate;
	}
	//to set average net receive(download) rate
	public void setAvgNETRxRate(double r){
		this.avg_net_rx_rate = r;
	}
	
	//to get average net transform(upload) rate
	public double getAvgNETTxRate(){
		return avg_net_tx_rate;
	}
	//to set average net transform(upload) rate
	public void setAvgNETTxRate(double r){
		this.avg_net_tx_rate = r;
	}

	//to get max net receive(download) rate
	public double getMaxNETRxRate(){
		return max_net_rx_rate;
	}
	//to set max net receive(download) rate
	public void setMaxNETRxRate(double r){
		this.max_net_rx_rate = r;
	}
	
	//to get max net transform(upload) rate
	public double getMaxNETTxRate(){
		return max_net_tx_rate;
	}
	//to set max net transform(upload) rate
	public void setMaxNETTxRate(double r){
		this.max_net_tx_rate = r;
	}
	
	//to get min net receive(download) rate
	public double getMinNETRxRate(){
		return min_net_rx_rate;
	}
	//to set min net receive(download) rate
	public void setMinNETRxRate(double r){
		this.min_net_rx_rate = r;
	}
	
	//to get min net transform(upload) rate
	public double getMinNETTxRate(){
		return min_net_tx_rate;
	}
	//to set min net transform(upload) rate
	public void setMinNETTxRate(double r){
		this.min_net_tx_rate = r;
	}

	//implements the Writable interface
	public void write(DataOutput out) throws IOException {
		Text.writeString(out, cardname);
//		out.writeUTF(cardname);
		out.writeInt(bandwidth);
		Text.writeString(out, duplex);
//		out.writeUTF(duplex);
		out.writeDouble(cur_net_rx_rate);
		out.writeDouble(cur_net_tx_rate);
		out.writeDouble(avg_net_rx_rate);
		out.writeDouble(avg_net_tx_rate);
		out.writeDouble(max_net_rx_rate);
		out.writeDouble(max_net_tx_rate);
		out.writeDouble(min_net_rx_rate);
		out.writeDouble(min_net_tx_rate);
	}
	public void readFields(DataInput in) throws IOException {
		cardname = Text.readString(in);
		bandwidth = in.readInt();
		duplex = Text.readString(in);
		cur_net_rx_rate = in.readDouble();
		cur_net_tx_rate = in.readDouble();
		avg_net_rx_rate = in.readDouble();
		avg_net_tx_rate = in.readDouble();
		max_net_rx_rate = in.readDouble();
		max_net_tx_rate = in.readDouble();
		min_net_rx_rate = in.readDouble();
		min_net_tx_rate = in.readDouble();
	}
	public static ServernodeNETStatus read(DataInput in) throws IOException{
		ServernodeNETStatus s = new ServernodeNETStatus();
		s.readFields(in);
		
		return s;
	}
}
