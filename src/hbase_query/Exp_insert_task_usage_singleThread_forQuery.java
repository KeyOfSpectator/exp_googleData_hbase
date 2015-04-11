package hbase_query;

import hbase_write_buffer.Hbase_createTable_googleData_util;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Vector;

import javacsv_operation.Javacsv_util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import com.csvreader.CsvReader;

import thread.OptThread_DyThreadSplit_PreRead;

public class Exp_insert_task_usage_singleThread_forQuery {
	
	static String familyName = "colFamily";
	static String col0 = "start_time";
	static String col1 = "end_time";
	static String col2 = "jobID";
	static String col3 = "task_index";
	static String col4 = "machineID";
	static String col5 = "CPU_rate";
	static String col6 = "canonical_memory_usage";
	static String col7 = "assigned_memory_usage";
	static String col8 = "unmapped_page_cache";
	static String col9 = "total_page_cache";
	static String col10 = "maximum_memory_usage";
	static String col11 = "disk_IO_time";
	static String col12 = "local_disk_space_usage";
	static String col13 = "maximum_CPU_rate";
	static String col14 = "maximum_disk_IO_time";
	static String col15 = "cycles_per_instruction";
	static String col16 = "memory_accesses_per_instruction";
	static String col17 = "sample_portion";
	static String col18 = "aggregation_type";
	static String col19 = "sampled_CPU_usage";

	public static ArrayList<byte[][]> byte_list;
	

	private static void storeToHbase_taskUsage(String csvFilePath , int lineCount, String site_PathStr, String tableName ) {
		// TODO Auto-generated method stub

		try{
			
			Configuration conf = HBaseConfiguration.create();
			
			HTableDescriptor htd = new HTableDescriptor(tableName);
			
			try {
				
//				rowKey 
				int i = 0;
				int i_origin = i;
				
				long total_write_time = 0;
				
				HTable table;
				
				table = new HTable(conf, htd.getName());
				table.setAutoFlush(false);
				table.setWriteBufferSize(1024*1024*2);
				
//					read data
					CsvReader reader = new CsvReader(csvFilePath, ',',
							Charset.forName("SJIS")); // 一般用这编码读就可以了

					reader.readHeaders(); // 跳过表头 如果需要表头的话，不要写这句。

					while (reader.readRecord()) { // 逐行读入除表头的数据
//						csvList.add(reader.getValues());
						

						long begin = System.currentTimeMillis();
						
//							rowKey
//							rowkey is jobID + task_index + start_time
						
							String jobID = reader.getValues()[2]; //补全到10位
							String task_index = reader.getValues()[3]; //补全到5位
							String start_time = reader.getValues()[0]; //补全到1位
							
							jobID = Hbase_createTable_googleData_util.ExpandString(jobID, 10);
							task_index = Hbase_createTable_googleData_util.ExpandString(task_index, 5);
							start_time = Hbase_createTable_googleData_util.ExpandString(start_time, 10);
						
							String rowKey = jobID+task_index+start_time;
							rowKey = Hbase_createTable_googleData_util.ExpandString(rowKey, 25);
							
//							System.out.println(i+" "+event);
						
							/*
							 * Put(byte[] row)
							 * Create a Put operation for the specified row.
							 */
//							Put p1 = new Put(Bytes.toBytes(intToString(i)));
							Put p1 = new Put(Bytes.toBytes(rowKey));
							p1.setWriteToWAL(false);
							
							/*
							 * add(byte[] family, byte[] qualifier, byte[] value) 
							 * Add the specified column and value to this Put operation.
							 */
							p1.add(Bytes.toBytes(familyName), Bytes.toBytes(col0), Bytes.toBytes( reader.getValues()[0]));
							p1.add(Bytes.toBytes(familyName), Bytes.toBytes(col1), Bytes.toBytes( reader.getValues()[1]));
							p1.add(Bytes.toBytes(familyName), Bytes.toBytes(col2), Bytes.toBytes( reader.getValues()[2]));
							p1.add(Bytes.toBytes(familyName), Bytes.toBytes(col3), Bytes.toBytes( reader.getValues()[3]));
							p1.add(Bytes.toBytes(familyName), Bytes.toBytes(col4), Bytes.toBytes( reader.getValues()[4]));
							p1.add(Bytes.toBytes(familyName), Bytes.toBytes(col5), Bytes.toBytes( reader.getValues()[5]));
							p1.add(Bytes.toBytes(familyName), Bytes.toBytes(col6), Bytes.toBytes( reader.getValues()[6]));
							p1.add(Bytes.toBytes(familyName), Bytes.toBytes(col7), Bytes.toBytes( reader.getValues()[7]));
							p1.add(Bytes.toBytes(familyName), Bytes.toBytes(col8), Bytes.toBytes( reader.getValues()[8]));
							p1.add(Bytes.toBytes(familyName), Bytes.toBytes(col9), Bytes.toBytes( reader.getValues()[9]));
							p1.add(Bytes.toBytes(familyName), Bytes.toBytes(col10), Bytes.toBytes( reader.getValues()[10]));
							p1.add(Bytes.toBytes(familyName), Bytes.toBytes(col11), Bytes.toBytes( reader.getValues()[11]));
							p1.add(Bytes.toBytes(familyName), Bytes.toBytes(col12), Bytes.toBytes( reader.getValues()[12]));
							p1.add(Bytes.toBytes(familyName), Bytes.toBytes(col13), Bytes.toBytes( reader.getValues()[13]));
							p1.add(Bytes.toBytes(familyName), Bytes.toBytes(col14), Bytes.toBytes( reader.getValues()[14]));
							p1.add(Bytes.toBytes(familyName), Bytes.toBytes(col15), Bytes.toBytes( reader.getValues()[15]));
							p1.add(Bytes.toBytes(familyName), Bytes.toBytes(col16), Bytes.toBytes( reader.getValues()[16]));
							p1.add(Bytes.toBytes(familyName), Bytes.toBytes(col17), Bytes.toBytes( reader.getValues()[17]));
							p1.add(Bytes.toBytes(familyName), Bytes.toBytes(col18), Bytes.toBytes( reader.getValues()[18]));
							p1.add(Bytes.toBytes(familyName), Bytes.toBytes(col19), Bytes.toBytes( reader.getValues()[19]));
							
							table.put(p1);
							
//							System.out.println(i);
							
//						}//end for
						long end = System.currentTimeMillis();
						total_write_time += (end - begin);
						if(i<lineCount)
						i++;
						
						
						
					}
					reader.close();
					
				table.flushCommits();
				
				System.out.println("[Info]"
						+ " [Time] HBase write time : " + total_write_time);
				System.out.println("[Thread]"
						+ " [Line] HBase write Line : " + (i-i_origin));
			}
			catch (Exception ex) {
				System.out.println(ex);
			}
			
		}//end try
		catch (Exception e)
        {
            e.printStackTrace();
        }//end catch
	}
	
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
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		if(args.length!=4){
			System.out.print("/* \n"
					+ " * Exp for insert task_usage\n"
					+ " * rowkey is job + task + start_time\n"
					+ "\n"
					+ " * DyThread DySplit PreRead\n"
					+ " * Data is 50WLine\n"
					+ " * \n"
					+ " * param:\n"
					+ " * args[0] : csvFilePath\n"
					+ " * args[1] : hbase-site Path\n"
					+ " * args[2] : table Name\n"
					+ " * args[3] : data lineNum\n"
					+ " * /\n");
			return;
		}
		final String csvFilePath = args[0];
		final String site_PathStr = args[1];
		final String tableName = args[2];
		final String lineNum = args[3];
		final int lineCount = Integer.parseInt(lineNum);
		
		System.out.print(""
				+ " * Exp for insert task_usage\n"
				+ " * rowkey is job + task + start_time\n"
				+ "\n"
				+ "[param] csvFilePath : " + csvFilePath + "\n"
				+ "[param] site_PathStr : " + site_PathStr + "\n"
				+ "[param] tableName : " + tableName + "\n"
				+ "[param] lineNum : " + lineNum + "\n"
				+ ""
				+ "");
		
//		Pre Read
//		long begin = System.currentTimeMillis();
//		
//		ArrayList<String[]> data_list = Javacsv_util.readCsv(csvFilePath);
//		
//		long end = System.currentTimeMillis();
//		System.out.println("[Time] Read the file time : " + (end - begin));
		
//		String[] to byte[][]
//		begin = System.currentTimeMillis();
//		
//		byte_list = Javacsv_util.ArrayList_String2byte(data_list , 20);
//		
//		end = System.currentTimeMillis();
//		System.out.println("[Time] String[] to byte[][] : " + (end - begin));
		
		
//		createtable
		Configuration conf = HBaseConfiguration.create();
		HTableDescriptor htd = new HTableDescriptor(tableName);
		try {
			Hbase_createTable_googleData_util.hbase_createTable(site_PathStr, conf, htd, tableName);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
//		store
		long st = System.currentTimeMillis();
		
		storeToHbase_taskUsage(csvFilePath , lineCount , site_PathStr, tableName );
		
		long en = System.currentTimeMillis();
		System.out.println("Throughput: "
				+ ((1000.0) * (((double) (lineCount)) / ((double) (en - st))))
				+ " ops/sec");
		
	}
}
