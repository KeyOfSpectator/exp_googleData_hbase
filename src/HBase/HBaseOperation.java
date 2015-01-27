package HBase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

public class HBaseOperation {
	//.相关属性
	   private  Configuration conf ;
	   private  HBaseAdmin admin;
	  
	   public  HBaseOperation(Configuration conf) throws  IOException{
	      this.conf=HBaseConfiguration.create(conf);
	      this.admin =new HBaseAdmin(this.conf);
	   }
	   public HBaseOperation() throws IOException{
	      Configuration cnf = new Configuration();
	      this.conf=HBaseConfiguration.create(cnf);
	      this.admin=new HBaseAdmin(this.conf);
	   }
	  
	   //1.创建表
	   public void  createTable(String tableName,String colFamilies[]) throws IOException{
	 
	         if(this.admin.tableExists(tableName)){
	            System.out.println("Table: "+tableName+" already exists !");
	         }else{
	            HTableDescriptor dsc = new HTableDescriptor(tableName);
	            int len = colFamilies.length;
	            for(int i=0;i<len;i++){
	               HColumnDescriptor family = new HColumnDescriptor(colFamilies[i]);
	               dsc.addFamily(family);
	            }
	            admin.createTable(dsc);
	            System.out.println("创建表成功");
	         }
	   }
	   //2.删除表
	   public void deleteTable(String tableName) throws IOException{
	         if(this.admin.tableExists(tableName)){
	            admin.deleteTable(tableName);
	            System.out.println("删除表成功");
	         }else{
	            System.out.println("Table Not Exists !");
	         }
	   }
	   //3.插入一行记录
	   public void insertRecord(String tableName,String rowkey,String family,String qualifier,String value) throws IOException {
	  
	         HTable table = new HTable(this.conf,tableName);
	         
//	         by fsc
	         table.setAutoFlush(false);
//	         table.setWriteBufferSize(209715200);
	         
	         System.out.println("WriteBufferSize = " + table.getWriteBufferSize());
	         
	         long begin=System.currentTimeMillis();     
	         
	         Put  put = new Put(rowkey.getBytes());
	         put.add(family.getBytes(),qualifier.getBytes(),value.getBytes());
	         table.put(put);
	        
	         table.flushCommits();	//
	         
	         long end=System.currentTimeMillis();
	         
	         System.out.println("插入行成功");
	         System.out.println("HBase:"+(end-begin));
	   }
	   //4.删除一行记录
	   public void deleteRecord(String tableName,String rowkey) throws IOException{
	     
	         HTable table = new HTable(this.conf,tableName);
	         Delete del =new Delete(rowkey.getBytes());
	         table.delete(del);
	         System.out.println("删除行成功");
	   }
	   //5.获取一行记录
	   public Result getOneRecord(String tableName,String rowkey) throws IOException{
	         HTable table =new HTable(this.conf,tableName);
	         Get get =new Get(rowkey.getBytes());    
	         Result rs = table.get(get); 
	         return rs;
	   }
	   //6.获取所有记录
	    public List<Result> getAllRecord(String tableName) throws IOException{
	               
	       HTable table = new HTable(this.conf,tableName);
	       Scan scan = new Scan();
	       ResultScanner scanner = table.getScanner(scan);
	       List<Result> list =new ArrayList<Result>();
	        for(Result r:scanner){ 
	          list.add(r);
	        }
	        scanner.close();
	        return list;      
	    }

}
