package hbase_query;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class hbase_query {

	/***
	  * 根据主键rowKey查询一行数据
	  * get 'student','010'
	  */
	 public static void showOneRecordByRowKey(String tableName,String rowKey ,Configuration conf)
	 {
	    
	  try {
		  
		  long st = System.currentTimeMillis();
		  
		  HTable table=new HTable(conf,tableName);  
	        Get g = new Get(Bytes.toBytes(rowKey));  
	        Result r=table.get(g);  
	        for(KeyValue kv:r.raw()){  
//	            System.out.println("column: "+new String(kv.getColumn()));  
	            System.out.println("value: "+new String(kv.getValue()));  
	        }  
	 
	        long en = System.currentTimeMillis();
	        System.out.println("[Time] Total time : "
	        		+ (double) (en - st)
	        		+ " ms");
	        
	  } catch (IOException e) {
	   // TODO Auto-generated catch block
	   e.printStackTrace();
	  }
	  System.out.println("end===========showOneRecordByRowKey");
	  
	 }
	 
	 /****
	  * 使用scan查询所有数据
	  * @param tableName
	  */
	 public static void showAllRecordsBetweenRowKey(String tableName , Configuration conf , String StartRowKey , String StopRowKey)
	 {
	  System.out.println("start==============show All Records=============");
	  
	  try {
		  
		  long st = System.currentTimeMillis();
		  
		  HTable table=new HTable(conf,tableName);
		  
	   //Scan所有数据
	   Scan scan = new Scan();
	   scan.setStartRow(Bytes.toBytes(StartRowKey));
	   scan.setStopRow(Bytes.toBytes(StopRowKey));
	   ResultScanner rss = table.getScanner(scan);
	   
	   
	   for(Result r:rss){
	    System.out.println("\n row: "+new String(r.getRow()));
	    
	    for(KeyValue kv:r.raw()){
	     
	     System.out.println("family=>"+new String(kv.getFamily(),"utf-8")
	           +"  value=>"+new String(kv.getValue(),"utf-8")
	     +"  qualifer=>"+new String(kv.getQualifier(),"utf-8")
	     +"  timestamp=>"+kv.getTimestamp());    
	    }
	   }
	   rss.close();
	   
	   long en = System.currentTimeMillis();
       System.out.println("[Time] Total time : "
       		+ (double) (en - st)
       		+ " ms");
	   
	  } catch (IOException e) {
	   // TODO Auto-generated catch block
	   e.printStackTrace();
	  }  
	  System.out.println("\n end==============show All Records=============");
	 }
	 
	 
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		if(args.length!=5){
			System.out.print("/* \n"
					+ " * hbase query\n"
					+ " * \n"
					+ " * \n"
					+ " * param:\n"
					+ " * args[0] : singOfRange (which 1 is single and 2 is range) \n"
					+ " * args[1] : hbase-site.xml Site_Path \n"
					+ " * args[2] : table Name \n"
					+ " * args[3] : StartRowKey (which single query is the query rowkey) \n"
					+ " * args[4] : StopRowKey: \n"
					+ " * /\n");
			return;
		}
		
		final String singleOfRange = args[0]; // 1 single , 2 range
		final String site_PathStr = args[1];
		final String tableName = args[2];
		final String StartRowKey = args[3];
		final String StopRowKey = args[4];
		
		System.out.print("/* \n"
				+ " * hbase query\n"
				+ " * \n"
				+ " * \n"
				+ " * param:\n"
				+ " * [param] : singOfRange (which 1 is single and 2 is range): " + singleOfRange + " \n"
				+ " * [param] : hbase-site.xml Site_Path: " + site_PathStr + "\n"
				+ " * [param] : table Name : " + tableName + "\n"
				+ " * [param] : StartRowKey (which single query is the query rowkey) : " + StartRowKey + "\n"
				+ " * [param] : StopRowKey: " + StopRowKey + "\n"
				+ " * /\n");
		
		String Site_Path = "/home/fsc/hbase/hbase-0.98.9-hadoop2/conf/hbase-site.xml";
		
		Configuration conf = HBaseConfiguration.create();
		conf.addResource(new Path(Site_Path));
		
		if(singleOfRange.equals("1")){
			showOneRecordByRowKey(tableName,StartRowKey,conf);
		}
		else{
			showAllRecordsBetweenRowKey(tableName , conf , StartRowKey , StopRowKey);
		}
		System.out.println("[done] query done");
	}

}
