package thread;

import hbase_thoughput.Exp_thoughput_50WL_DyThreadSplit_PreRead;
import hbase_thoughput.Exp_thoughput_57M_32Threads;

import java.nio.charset.Charset;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import com.csvreader.CsvReader;

/*
 * used by Exp_thoughput_57M_32Threads
 * opt read and store the data
 */

public class OptThread_DyThreadSplit_PreRead extends Thread{
	private int threadId;
	private String csvFilePath;
	private String site_PathStr;
	private String tableName;
	private int opt_per_thread;
//	private ArrayList<byte[][]> byte_list;
	
	static String familyName = "colFamily";
	static String col0 = "time";
	static String col1 = "missing";
	static String col2 = "job";
	static String col3 = "task";
	static String col4 = "machine";
	static String col5 = "event";
	static String col6 = "user";
	static String col7 = "scheduling";
	static String col8 = "priority";
	static String col9 = "cpu";
	static String col10 = "memory";
	static String col11 = "disk";
	static String col12 = "different";
	
	public OptThread_DyThreadSplit_PreRead(int threadId ,int threadCount , String site_PathStr , String tableName){
		this.threadId = threadId;
//		this.csvFilePath = FolderPath+String.valueOf(threadId)+".csv";
		this.site_PathStr = site_PathStr;
		this.tableName = tableName;
		this.opt_per_thread = 500000/threadCount;
//		this.byte_list = Exp_thoughput_50WL_DyThreadSplit_PreRead.byte_list;
	}
	public void run(){
		
		try{
			System.out.println("[Thread Info]"
					+ " threadId = " + this.threadId
					+ " started");
			
			Configuration conf = HBaseConfiguration.create();
			
			HTableDescriptor htd = new HTableDescriptor(tableName);
			
//			hbase_create_table( site_PathStr , tableName , conf , htd);
			
			try {
				// String csvFilePath = "/home/fsc/ubuntu_exp/0.csv";
//				CsvReader reader = new CsvReader(csvFilePath, ',' , Charset.forName("SJIS")); // 一般用这编码读就可以了
				
//				reader.readHeaders(); // 跳过表头 如果需要表头的话，不要写这句。
				
//				rowKey 
				int i = opt_per_thread*threadId;
				int i_origin = i;
				
				long total_write_time = 0;
				
				HTable table;
//				synchronized(Exp_thoughput_57M_32Threads.tableLock){
//					table = new HTable(conf, htd.getName());
//					table.setAutoFlush(false);
//					table.setWriteBufferSize(1024*1024*2);
//				}
				
				table = new HTable(conf, htd.getName());
				table.setAutoFlush(false);
				table.setWriteBufferSize(1024*1024*2);
				
				// table.setWriteBufferSize(209715200);
//				System.out.println("[Thread Info]"
//						+ " threadId = " + this.threadId
//						+ " WriteBufferSize :  " + table.getWriteBufferSize());
				
//				System.out.println("[Thread Info]"
//						+ " threadId = " + this.threadId
//						+ " csvFilePath :  " + csvFilePath);
				
				System.out.println("[Thread Info]"
				+ " threadId = " + this.threadId
				+ " start to insert   ");
				
				
				for(int j=0;j<opt_per_thread;j++) { // 逐行插入数据
					
					long begin = System.currentTimeMillis();
					
						/*
						 * Put(byte[] row)
						 * Create a Put operation for the specified row.
						 */
						Put p1 = new Put(Bytes.toBytes(intToString(i)));
						p1.setWriteToWAL(false);
						
						/*
						 * add(byte[] family, byte[] qualifier, byte[] value) 
						 * Add the specified column and value to this Put operation.
						 */
						p1.add(Bytes.toBytes(familyName), Bytes.toBytes(col0), Exp_thoughput_50WL_DyThreadSplit_PreRead.byte_list.get(i)[0]);
						p1.add(Bytes.toBytes(familyName), Bytes.toBytes(col1), Exp_thoughput_50WL_DyThreadSplit_PreRead.byte_list.get(i)[1]);
						p1.add(Bytes.toBytes(familyName), Bytes.toBytes(col2), Exp_thoughput_50WL_DyThreadSplit_PreRead.byte_list.get(i)[2]);
						p1.add(Bytes.toBytes(familyName), Bytes.toBytes(col3), Exp_thoughput_50WL_DyThreadSplit_PreRead.byte_list.get(i)[3]);
						p1.add(Bytes.toBytes(familyName), Bytes.toBytes(col4), Exp_thoughput_50WL_DyThreadSplit_PreRead.byte_list.get(i)[4]);
						p1.add(Bytes.toBytes(familyName), Bytes.toBytes(col5), Exp_thoughput_50WL_DyThreadSplit_PreRead.byte_list.get(i)[5]);
						p1.add(Bytes.toBytes(familyName), Bytes.toBytes(col6), Exp_thoughput_50WL_DyThreadSplit_PreRead.byte_list.get(i)[6]);
						p1.add(Bytes.toBytes(familyName), Bytes.toBytes(col7), Exp_thoughput_50WL_DyThreadSplit_PreRead.byte_list.get(i)[7]);
						p1.add(Bytes.toBytes(familyName), Bytes.toBytes(col8), Exp_thoughput_50WL_DyThreadSplit_PreRead.byte_list.get(i)[8]);
						p1.add(Bytes.toBytes(familyName), Bytes.toBytes(col9), Exp_thoughput_50WL_DyThreadSplit_PreRead.byte_list.get(i)[9]);
						p1.add(Bytes.toBytes(familyName), Bytes.toBytes(col10), Exp_thoughput_50WL_DyThreadSplit_PreRead.byte_list.get(i)[10]);
						p1.add(Bytes.toBytes(familyName), Bytes.toBytes(col11), Exp_thoughput_50WL_DyThreadSplit_PreRead.byte_list.get(i)[11]);
						p1.add(Bytes.toBytes(familyName), Bytes.toBytes(col12), Exp_thoughput_50WL_DyThreadSplit_PreRead.byte_list.get(i)[12]);
					
						
						table.put(p1);
						
//						System.out.println(i);
						
//					}//end for
					long end = System.currentTimeMillis();
					total_write_time += (end - begin);
					if(i<499998)
					i++;
//					//print every line logs
//					System.out.println("[Thread Info]"
//							+ " threadId = " + this.threadId
//							+ " [Line] HBase write line : " + i);
				}//end while
				
//				synchronized(Exp_thoughput_57M_32Threads.tableLock){
//					table.flushCommits();
//				}
				table.flushCommits();
				
				System.out.println("[Thread Info]"
						+ " threadId = " + this.threadId
						+ " [Time] HBase write time : " + total_write_time);
				System.out.println("[Thread Info]"
						+ " threadId = " + this.threadId
						+ " [Line] HBase write Line : " + (i-i_origin));
//				reader.close();
			}
			catch (Exception ex) {
				System.out.println(ex);
			}
			
			
		}//end try
		catch (Exception e)
        {
            e.printStackTrace();
        }//end catch
	}//end run
	
	
	static public String intToString(int x) {
		String result = String.valueOf(x);
		int size = result.length();
//		1000W line rowkey adapt
		while (size < 8) {
			size++;
			result = "0" + result;
		}
		return result;
	}
}