package hbase_thoughput;

import hbase_write_buffer.Hbase_createTable_googleData_util;

import java.io.IOException;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;

import thread.OptThread_split3Table_DynamicNum;

/**thoughput for split to 2 tables
 * 50M 50WLine data
 * DyThread split for hbase
 * @author fsc
 *
 */
public class Exp_thoughput_split3table_DynamicThreads_50WLine {

	public static void main(String[] args) {

		// TODO Auto-generated method stub

//		System.out.print("/* \n"
//				+ " * Exp for thoughput\n"
//				+ " * Data is 50WLine\n"
//				+ " * "
//				+ " * param:\n"
//				+ " * args[0] : csvFolderPath\n"
//				+ " * args[1] : hbase-site Path\n"
//				+ " * args[2] : thread Num\n"
//				+ " * args[3] : hbase create table split num\n"
//				+ " * args[4] : table Name\n"
//				+ " * /\n");
		
		// param
		// final int threadCount = Integer.parseInt(args[3]);
		final String csvFolderPath = args[0];
		final String site_PathStr = args[1];
		final String threadNum = args[2];
		final String SplitNum_Str = args[3];
		final String tableName = args[4];
		final int threadCount = Integer.parseInt(threadNum);
		final int SplitNum = Integer.parseInt(SplitNum_Str);
		final int opcount = 500000/threadCount;
		
		String tableName1 = tableName+"_1";
		String tableName2 = tableName+"_2";
		String tableName3 = tableName+"_3";
		
		System.out.print("/* \n"
				+ " * Exp for thoughput\n"
				+ " * Data is 50WLine\n"
				+ " * \n"
				+ " * param:\n"
				+ " * [param] : thread Num: " + threadNum + "\n"
				+ " * [param] : split num : " + SplitNum_Str + "\n"
				+ " * [param] : table Name: " + tableName + "\n"
				+ " * /\n");
//		createtable
		Configuration conf = HBaseConfiguration.create();
		HTableDescriptor htd1 = new HTableDescriptor(tableName1);
		HTableDescriptor htd2 = new HTableDescriptor(tableName2);
		HTableDescriptor htd3 = new HTableDescriptor(tableName3);
		try {
			Hbase_createTable_googleData_util.hbase_createTable(site_PathStr, conf, htd1, tableName, 0, 500000, SplitNum);
			Hbase_createTable_googleData_util.hbase_createTable(site_PathStr, conf, htd2, tableName, 0, 500000, SplitNum);
			Hbase_createTable_googleData_util.hbase_createTable(site_PathStr, conf, htd3, tableName, 0, 500000, SplitNum);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
//		store
		Vector<Thread> allthreads = new Vector<Thread>();

		for (int i = 0; i < threadCount; i++) {
			/*
			 * OptThread_DynamicNum(int threadId ,int threadNum, String FolderPath , String site_PathStr
			 * , String tableName)
			 */
			Thread t = new OptThread_split3Table_DynamicNum(i,threadCount , csvFolderPath, site_PathStr, tableName );
			allthreads.add(t);

		}
		long st = System.currentTimeMillis();
		for (Thread t : allthreads) {
			t.start();
		}

		for (Thread t : allthreads) {
			try {
				t.join();
			} catch (InterruptedException e) {
			}
		}
		long en = System.currentTimeMillis();
		System.out.println("Total time : "
				+ (double) (en - st)
				+ " ms");
		System.out.println("Throughput: "
						+ ((1000.0) * (((double) (opcount * threadCount)) / ((double) (en - st))))
						+ " ops/sec");

	}

}
