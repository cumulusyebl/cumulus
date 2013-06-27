package org.apache.hadoop.hdfs.server.datanode;
/* Tongxin 
 *  2013.4.2
 */

import java.io.*;

import org.aspectj.org.eclipse.jdt.core.dom.ThisExpression;

public class DatanodeStat {

	/**
	 * @param args
	 */
	protected double cpuUsed;
	protected double ioUsed;
	protected double memUsed;
	public DatanodeStat()
	{
		
	}
	public DatanodeStat(double cpu,double io,double mem)
	{
		this.cpuUsed=cpu;
		this.ioUsed=io;
		this.memUsed=mem;
	}
	public double getCpuUsed()
	{
		return this.cpuUsed;
	}
	public void setCpuUsed(double cpu)
	{
		this.cpuUsed=cpu;
	}
	public double getIoUsed()
	{
		return this.ioUsed;
	}
	public void setIoUsed(double io)
	{
		this.ioUsed=io;
	}
	public double getMemUsed()
	{
		return this.memUsed;
	}
	public void setMemUsed(double mem)
	{
		this.memUsed=mem;
	}
	public void calCpuUsed()throws Exception
	{
		double cpuUsed=0;
		Runtime rt=Runtime.getRuntime();
		String command="cat /proc/stat";
		Process pro1,pro2;
		BufferedReader in1,in2;
	 try{
		   //First Time
		   pro1=rt.exec(command);
			in1=new BufferedReader(new InputStreamReader(pro1.getInputStream()));
			String str=null;
			String[] strArray=null;
			double idleCpuTime1=0,totalCpuTime1=0;
			while((str=in1.readLine())!=null)
			 {

				if(str.startsWith("cpu")){
					//System.out.println(str);
					str=str.trim();
					strArray=str.split("\\s+");
					idleCpuTime1=Double.parseDouble(strArray[4]);
					 for(int i=0;i<strArray.length;i++)
					 {
						 if(!"cpu".equals(strArray[i]))
							 totalCpuTime1+=Double.parseDouble(strArray[i]);
					 }
					 break;
				}
		     }
			
			
			try{
				Thread.sleep(1000);
			}catch(InterruptedException e1){
				e1.printStackTrace();
			}
			//Second Time
			pro2=rt.exec(command);
			in2=new BufferedReader(new InputStreamReader(pro2.getInputStream()));
			double idleCpuTime2=0,totalCpuTime2=0;
			while((str=in2.readLine())!=null)
			 {
				 //System.out.println(str);
				if(str.startsWith("cpu")){
					//System.out.println(str);
					str=str.trim();
					strArray=str.split("\\s+");
					idleCpuTime2=Double.parseDouble(strArray[4]);
					 for(int i=0;i<strArray.length;i++)
					 {
						 if(!"cpu".equals(strArray[i]))
							 totalCpuTime2+=Double.parseDouble(strArray[i]);
					 }
					 break;
				}
		     }
			if(idleCpuTime1!=0 &&totalCpuTime1!=0 &&idleCpuTime2!=0&&totalCpuTime2!=0)
			{
				 cpuUsed=1-(idleCpuTime2-idleCpuTime1)/(totalCpuTime2-totalCpuTime1);
			}
			in1.close();
			in2.close();
			pro1.destroy();
			pro2.destroy();
		 }
	 catch(Exception e)
		{
			  e.printStackTrace();
		}
		this.setCpuUsed(cpuUsed);
	 }
	
	public void calMemUsed()throws Exception
	{
		double totalMemory=0;
		double freeMemory=0;
		double usedMemory=0;
		Runtime rt=Runtime.getRuntime();
		Process p=rt.exec("cat /proc/meminfo");
		BufferedReader in=null;
		
		try{
			in=new BufferedReader(new InputStreamReader(p.getInputStream()));
			String str=null;
			String[] strArray=null;
			
			while((str=in.readLine())!=null)
			{
				//System.out.println(str);
				strArray=str.split("\\s+");
				if(strArray[0].startsWith("MemTotal")){
					totalMemory=Double.parseDouble(strArray[1]);
				}
				if(strArray[0].startsWith("MemFree")){
					freeMemory=Double.parseDouble(strArray[1]);
				}
            if(strArray[0].startsWith("Buffers"))
                {
            	freeMemory+=Double.parseDouble(strArray[1]);
                }
            if(strArray[0].startsWith("Cached"))
               {
                freeMemory+=Double.parseDouble(strArray[1]);  
               }
			}
		}catch(Exception e)
		{
			e.printStackTrace();
		}
		this.setMemUsed(1-freeMemory/totalMemory);
		//System.out.println("TotalMemory: "+totalMemory+" FreeMemory: "+freeMemory+" UsedMemory: "+usedMemory);
	}
	
	public void calIoUsed()throws Exception
	{
		double ioUsed=0;
		
		Runtime rt=Runtime.getRuntime();
		Process p=rt.exec("iostat -d -x");
		BufferedReader in=null;
		try
		{
			in=new BufferedReader(new InputStreamReader(p.getInputStream()));
			String str=null;
			String[] strArray=null;
			int count=0;
			while((str=in.readLine())!=null)
			{
				count++;
				if(count>=4)
				{
					//System.out.println(str);
					strArray=str.split("\\s+");
					//for(int i=0;i<strArray.length;i++)
					//	System.out.print(strArray[i]+" ");
					if(strArray.length>1)
					{
						double temp=Double.parseDouble(strArray[strArray.length-1]);
						ioUsed=(ioUsed>temp)?ioUsed:temp;
					}
				}
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		this.setIoUsed(ioUsed);
	}
	public static void main(String[] args)throws Exception{
		// TODO Auto-generated method stub
             DatanodeStat dnstat=new DatanodeStat();
             dnstat.calCpuUsed();
             System.out.println(dnstat.getCpuUsed());
             dnstat.calMemUsed();
             System.out.println(dnstat.getMemUsed());
             dnstat.calIoUsed();
             System.out.println(dnstat.getIoUsed());
	}

}
