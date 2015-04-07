package thread;

import java.nio.charset.Charset;

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

public class OptThread_split2Table_DynamicNum extends Thread{
	private int threadId;
	private String csvFilePath;
	private String site_PathStr;
	private String tableName;
	private int opt_per_thread;
	
	private String tableName1;
	private String tableName2;
	
//	table1 col 0,1,2,3,4,5,6,7,8
//	table2 col 0,2,3,5,9,10,11,12
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
	
	public OptThread_split2Table_DynamicNum(int threadId ,int threadCount , String FolderPath , String site_PathStr , String tableName){
		this.threadId = threadId;
		this.csvFilePath = FolderPath+String.valueOf(threadId)+".csv";
		this.site_PathStr = site_PathStr;
		this.tableName = tableName;
		this.tableName1 = tableName+"_1";
		this.tableName2 = tableName+"_2";
		this.opt_per_thread = 500000/threadCount;
	}
	public void run(){
		
		try{
			
			Configuration conf = HBaseConfiguration.create();
			
			HTableDescriptor htd1 = new HTableDescriptor(tableName1);
			HTableDescriptor htd2 = new HTableDescriptor(tableName2);
			
			try {
				CsvReader reader = new CsvReader(csvFilePath, ',' , Charset.forName("SJIS")); // 一般用这编码读就可以了
				
				reader.readHeaders(); // 跳过表头 如果需要表头的话，不要写这句。
				
//				rowKey 
				int i = opt_per_thread*threadId;
				int i_origin = i;
				
				long total_write_time = 0;
				
				HTable table1;
					table1 = new HTable(conf, htd1.getName());
					table1.setAutoFlush(false);
					table1.setWriteBufferSize(1024*1024*2);
				HTable table2;
					table2 = new HTable(conf, htd2.getName());
					table2.setAutoFlush(false);
					table2.setWriteBufferSize(1024*1024*2);
				
				while (reader.readRecord()) { // 逐行读入除表头的数据
					
					long begin = System.currentTimeMillis();
					
						/*
						 * Put(byte[] row)
						 * Create a Put operation for the specified row.
						 */
						Put p1 = new Put(Bytes.toBytes(intToString(i)));
						p1.setWriteToWAL(false);
						Put p2 = new Put(Bytes.toBytes(intToString(i)));
						p2.setWriteToWAL(false);
						
						/*
						 * add(byte[] family, byte[] qualifier, byte[] value) 
						 * Add the specified column and value to this Put operation.
						 */
//						table1 col 0,1,2,3,4,5,6,7,8
//						table2 col 0,2,3,5,9,10,11,12
						p1.add(Bytes.toBytes(familyName), Bytes.toBytes(col0), Bytes.toBytes( reader.getValues()[0] ) );
						p1.add(Bytes.toBytes(familyName), Bytes.toBytes(col1), Bytes.toBytes( reader.getValues()[1] ) );
						p1.add(Bytes.toBytes(familyName), Bytes.toBytes(col2), Bytes.toBytes( reader.getValues()[2] ) );
						p1.add(Bytes.toBytes(familyName), Bytes.toBytes(col3), Bytes.toBytes( reader.getValues()[3] ) );
						p1.add(Bytes.toBytes(familyName), Bytes.toBytes(col4), Bytes.toBytes( reader.getValues()[4] ) );
						p1.add(Bytes.toBytes(familyName), Bytes.toBytes(col5), Bytes.toBytes( reader.getValues()[5] ) );
						p1.add(Bytes.toBytes(familyName), Bytes.toBytes(col6), Bytes.toBytes( reader.getValues()[6] ) );
						p1.add(Bytes.toBytes(familyName), Bytes.toBytes(col7), Bytes.toBytes( reader.getValues()[7] ) );
						p1.add(Bytes.toBytes(familyName), Bytes.toBytes(col8), Bytes.toBytes( reader.getValues()[8] ) );
						
						
						p2.add(Bytes.toBytes(familyName), Bytes.toBytes(col0), Bytes.toBytes( reader.getValues()[0] ) );
						p2.add(Bytes.toBytes(familyName), Bytes.toBytes(col2), Bytes.toBytes( reader.getValues()[2] ) );
						p2.add(Bytes.toBytes(familyName), Bytes.toBytes(col3), Bytes.toBytes( reader.getValues()[3] ) );
						p2.add(Bytes.toBytes(familyName), Bytes.toBytes(col5), Bytes.toBytes( reader.getValues()[5] ) );
						p2.add(Bytes.toBytes(familyName), Bytes.toBytes(col9), Bytes.toBytes( reader.getValues()[9] ) );
						p2.add(Bytes.toBytes(familyName), Bytes.toBytes(col10), Bytes.toBytes( reader.getValues()[10] ) );
						p2.add(Bytes.toBytes(familyName), Bytes.toBytes(col11), Bytes.toBytes( reader.getValues()[11] ) );
						p2.add(Bytes.toBytes(familyName), Bytes.toBytes(col12), Bytes.toBytes( reader.getValues()[12] ) );
						
						table1.put(p1);
						table2.put(p2);
						
//					}//end for
					long end = System.currentTimeMillis();
					total_write_time += (end - begin);
					i++;
//					//print every line logs
//					System.out.println("[Thread Info]"
//							+ " threadId = " + this.threadId
//							+ " [Line] HBase write line : " + i);
				}//end while
				
					table1.flushCommits();
					table2.flushCommits();
				
//				System.out.println("[Thread Info]"
//						+ " threadId = " + this.threadId
//						+ " [Time] HBase write time : " + total_write_time);
//				System.out.println("[Thread Info]"
//						+ " threadId = " + this.threadId
//						+ " [Line] HBase write Line : " + (i-i_origin));
				reader.close();
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