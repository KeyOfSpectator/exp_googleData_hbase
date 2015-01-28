package hbase_thoughput;

import hbase_write_buffer.Hbase_write_buffer_util;

import java.io.IOException;
import java.util.ArrayList;

import javacsv_operation.Javacsv_util;

public class Exp_thoughput {

	
/*
 * $java -jar thoughput_hbase.jar XXX XXX XXX
 * 
 * This is the demo to write google monitor data
 * thoughput method 
 * 
 * author: fsc
 * 2015-1-27
 * 
 * args	
 * args[0] :  csvFilePath 		  	exp:"/home/fsc/ubuntu_exp/0.csv";
 * args[1] :  hbase-site_PathStr  	exp:"/home/fsc/hbase/hbase-0.98.9-hadoop2/conf/hbase-site.xml";
 * args[2] :  tableName				exp:"google_exp_57M_singleMachine";
 * 
 */
	
//	 Config:
	
public static String csvFilePath = "/home/fsc/ubuntu_exp/0.csv";	

public static String hdfsSite_PathStr = "/home/fsc/hbase/hbase-0.98.9-hadoop2/conf/hbase-site.xml";

public static String tableName = "google_exp_57M_singleMachine";
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		
//		read file
		long begin = System.currentTimeMillis();
		
		ArrayList<String[]> data_list = Javacsv_util.readCsv(args[0]);
		
		long end = System.currentTimeMillis();
		System.out.println("[Time] Read the file time : " + (end - begin));
		
//		String[] to byte[][]
		begin = System.currentTimeMillis();
		
		ArrayList<byte[][]> byte_list = Javacsv_util.ArrayList_String2byte(data_list);
		
		end = System.currentTimeMillis();
		System.out.println("[Time] String[] to byte[][] : " + (end - begin));
		
//		write to hbase
		try {
			Hbase_write_buffer_util.hbase_write_ubuntuExp( args[1], args[2], byte_list);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}
