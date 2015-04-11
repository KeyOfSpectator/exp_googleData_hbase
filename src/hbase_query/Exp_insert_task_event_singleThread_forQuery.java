package hbase_query;

import hbase_write_buffer.Hbase_createTable_googleData_util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Vector;

import javacsv_operation.Javacsv_util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import thread.OptThread_DyThreadSplit_PreRead;

public class Exp_insert_task_event_singleThread_forQuery {
	
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

	public static ArrayList<byte[][]> byte_list;
	

	private static void storeToHbase_taskEvent(int lineCount, String site_PathStr, String tableName , ArrayList<String[]> data_list) {
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
				
				for(int j=0;j<lineCount;j++) { // 逐行插入数据
					
					long begin = System.currentTimeMillis();
					
//						rowKey
//						rowkey is job + task + event
					
						String job = data_list.get(i)[2]; //补全到10位
						String task = data_list.get(i)[3]; //补全到5位
						String event = data_list.get(i)[5]; //补全到1位
						
						job = Hbase_createTable_googleData_util.ExpandString(job, 10);
						task = Hbase_createTable_googleData_util.ExpandString(task, 5);
						event = Hbase_createTable_googleData_util.ExpandString(event, 1);
					
						String rowKey = job+task+event;
						rowKey = Hbase_createTable_googleData_util.ExpandString(rowKey, 16);
						
						System.out.println(i+" "+event);
					
						/*
						 * Put(byte[] row)
						 * Create a Put operation for the specified row.
						 */
//						Put p1 = new Put(Bytes.toBytes(intToString(i)));
						Put p1 = new Put(Bytes.toBytes(rowKey));
						p1.setWriteToWAL(false);
						
						/*
						 * add(byte[] family, byte[] qualifier, byte[] value) 
						 * Add the specified column and value to this Put operation.
						 */
						p1.add(Bytes.toBytes(familyName), Bytes.toBytes(col0), Exp_insert_task_event_singleThread_forQuery.byte_list.get(i)[0]);
						p1.add(Bytes.toBytes(familyName), Bytes.toBytes(col1), Exp_insert_task_event_singleThread_forQuery.byte_list.get(i)[1]);
						p1.add(Bytes.toBytes(familyName), Bytes.toBytes(col2), Exp_insert_task_event_singleThread_forQuery.byte_list.get(i)[2]);
						p1.add(Bytes.toBytes(familyName), Bytes.toBytes(col3), Exp_insert_task_event_singleThread_forQuery.byte_list.get(i)[3]);
						p1.add(Bytes.toBytes(familyName), Bytes.toBytes(col4), Exp_insert_task_event_singleThread_forQuery.byte_list.get(i)[4]);
						p1.add(Bytes.toBytes(familyName), Bytes.toBytes(col5), Exp_insert_task_event_singleThread_forQuery.byte_list.get(i)[5]);
						p1.add(Bytes.toBytes(familyName), Bytes.toBytes(col6), Exp_insert_task_event_singleThread_forQuery.byte_list.get(i)[6]);
						p1.add(Bytes.toBytes(familyName), Bytes.toBytes(col7), Exp_insert_task_event_singleThread_forQuery.byte_list.get(i)[7]);
						p1.add(Bytes.toBytes(familyName), Bytes.toBytes(col8), Exp_insert_task_event_singleThread_forQuery.byte_list.get(i)[8]);
						p1.add(Bytes.toBytes(familyName), Bytes.toBytes(col9), Exp_insert_task_event_singleThread_forQuery.byte_list.get(i)[9]);
						p1.add(Bytes.toBytes(familyName), Bytes.toBytes(col10), Exp_insert_task_event_singleThread_forQuery.byte_list.get(i)[10]);
						p1.add(Bytes.toBytes(familyName), Bytes.toBytes(col11), Exp_insert_task_event_singleThread_forQuery.byte_list.get(i)[11]);
						p1.add(Bytes.toBytes(familyName), Bytes.toBytes(col12), Exp_insert_task_event_singleThread_forQuery.byte_list.get(i)[12]);
						
						table.put(p1);
						
//						System.out.println(i);
						
//					}//end for
					long end = System.currentTimeMillis();
					total_write_time += (end - begin);
					if(i<lineCount)
					i++;
				}//end while
				
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
		if(args.length!=5){
			System.out.print("/* \n"
					+ " * Exp for insert task_event\n"
					+ " * rowkey is job + task + event\n"
					+ "\n"
					+ " * DyThread DySplit PreRead\n"
					+ " * Data is 50WLine\n"
					+ " * \n"
					+ " * param:\n"
					+ " * args[0] : csvFilePath\n"
					+ " * args[1] : hbase-site Path\n"
					+ " * args[2] : hbase create table split num\n"
					+ " * args[3] : table Name\n"
					+ " * args[4] : data lineNum\n"
					+ " * /\n");
			return;
		}
		final String csvFilePath = args[0];
		final String site_PathStr = args[1];
		final String SplitNum_Str = args[2];
		final String tableName = args[3];
		final String lineNum = args[4];
		final int lineCount = Integer.parseInt(lineNum);
		final int SplitNum = Integer.parseInt(SplitNum_Str);
		
		System.out.print(""
				+ " * Exp for insert task_event\n"
				+ " * rowkey is job + task + event\n"
				+ "\n"
				+ "[param] csvFilePath : " + csvFilePath + "\n"
				+ "[param] site_PathStr : " + site_PathStr + "\n"
				+ "[param] SplitNum_Str : " + SplitNum_Str + "\n"
				+ "[param] tableName : " + tableName + "\n"
				+ "[param] lineNum : " + lineNum + "\n"
				+ ""
				+ "");
		
//		Pre Read
		long begin = System.currentTimeMillis();
		
		ArrayList<String[]> data_list = Javacsv_util.readCsv(csvFilePath);
		
		long end = System.currentTimeMillis();
		System.out.println("[Time] Read the file time : " + (end - begin));
		
//		String[] to byte[][]
		begin = System.currentTimeMillis();
		
		byte_list = Javacsv_util.ArrayList_String2byte(data_list);
		
		end = System.currentTimeMillis();
		System.out.println("[Time] String[] to byte[][] : " + (end - begin));
		
		
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
		
		storeToHbase_taskEvent(lineCount , site_PathStr, tableName , data_list);
		
		long en = System.currentTimeMillis();
		System.out.println("Throughput: "
				+ ((1000.0) * (((double) (lineCount)) / ((double) (en - st))))
				+ " ops/sec");
		
	}
}
