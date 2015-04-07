package hbase_query;

import hbase_write_buffer.Hbase_createTable_googleData_util;
import hbase_write_buffer.Hbase_write_buffer_util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;

import thread.OptThread;
import thread.OptThread_DynamicNum;
import javacsv_operation.Javacsv_util;

public class Exp_thoughput_50WLine_DynamicThreads {

	/*
	 * $java -jar thoughput_hbase.jar XXX XXX XXX XXX
	 * 
	 * This is the demo to write google monitor data thoughput method
	 * 
	 * author: fsc 2015-1-27
	 * 
	 * args args[0] : csvFolderPath (!) exp:"/home/fsc/ubuntu_exp/"; warning:
	 * the files are below the FolderPath and sequenced by 0.csv args[1] :
	 * hbase-site_PathStr
	 * exp:"/home/fsc/hbase/hbase-0.98.9-hadoop2/conf/hbase-site.xml"; args[2] :
	 * tableName exp:"google_exp_57M_singleMachine"; args[3] : threadCount
	 * exp:"32";
	 */

	// Config:

	final public static String csvFolderPath = "/home/fsc/ubuntu_exp/0.csv";

	public static String hdfsSite_PathStr = "/home/fsc/hbase/hbase-0.98.9-hadoop2/conf/hbase-site.xml";

	public static String tableName = "google_exp_57M_singleMachine";

	/*
	 * public class optThread extends Thread{ private int threadId; public
	 * optThread(int threadId){ this.threadId = threadId; } public void run(){
	 * 
	 * try{ System.out.println("[Thread Info]" + " threadId = " + this.threadId
	 * + " started"); }//end try catch (Exception e) { e.printStackTrace();
	 * }//end catch }//end run
	 * 
	 * }
	 */

	public static final Object tableLock = new Object();// syn the table

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
		HTableDescriptor htd = new HTableDescriptor(tableName);
		try {
			Hbase_createTable_googleData_util.hbase_createTable(site_PathStr, conf, htd, tableName, 0, 500000, SplitNum);
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
			Thread t = new OptThread_DynamicNum(i,threadCount , csvFolderPath, site_PathStr, tableName );
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
