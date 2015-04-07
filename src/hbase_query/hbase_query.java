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
//	  HTablePool pool = new HTablePool(conf,100);
//	  HTable table = (HTable) pool.getTable(tableName);
	    
	  try {
//	   Get get = new Get(rowkey.getBytes()); //根据主键查询
//	   Result r = table.get(get);
//	   System.out.println("start===showOneRecordByRowKey==row: "+"\n");
//	   System.out.println("row: "+new String(r.getRow(),"utf-8"));
//	   
//	   for(KeyValue kv:r.raw()){
//	    //时间戳转换成日期格式
//	    String timestampFormat = new SimpleDateFormat("yyyy-MM-dd HH:MM:ss").format(new Date(kv.getTimestamp()));
//	       //System.out.println("===:"+timestampFormat+"  ==timestamp: "+kv.getTimestamp());
//	    System.out.println("\nKeyValue: "+kv);
//	    System.out.println("key: "+kv.getKeyString());
//	    
//	    System.out.println("family=>"+new String(kv.getFamily(),"utf-8")
//	          +"  value=>"+new String(kv.getValue(),"utf-8")
//	    +"  qualifer=>"+new String(kv.getQualifier(),"utf-8")
//	    +"  timestamp=>"+timestampFormat);
//	  }
		  
		  long st = System.currentTimeMillis();
		  
		  HTable table=new HTable(conf,tableName);  
	        Get g = new Get(Bytes.toBytes(rowKey));  
	        Result r=table.get(g);  
	        for(KeyValue kv:r.raw()){  
//	            System.out.println("column: "+new String(kv.getColumn()));  
	            System.out.println("value: "+new String(kv.getValue()));  
	        }  
	 
	        long en = System.currentTimeMillis();
	        System.out.println("Total time : "
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
	  
//	  HTablePool pool = new HTablePool(conf,1000);
//	  //创建table对象
//	  HTable table = (HTable) pool.getTable(tableName);
	  
//	  String startRowKey = "00300468";
//	  String stopRowKey = "00301468";
	  
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
       System.out.println("Total time : "
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
		
		String Site_Path = "/home/fsc/hbase/hbase-0.98.9-hadoop2/conf/hbase-site.xml";
		
		Configuration conf = HBaseConfiguration.create();
		conf.addResource(new Path(Site_Path));

		
//		showOneRecordByRowKey("google_exp_57M_singleMachine","00300468",conf);
		
		showAllRecordsBetweenRowKey("google_exp_57M_singleMachine" , conf , "00300468" , "00311468");
	}

}
