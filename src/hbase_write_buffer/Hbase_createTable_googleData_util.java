package hbase_write_buffer;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * @author fsc
 *
 */
public class Hbase_createTable_googleData_util {

	/*
	 * this is the util class create the hbase table 
	 * args[0] is the hdfsSite_PathStr (String) 
	 * args[1] is the Configuration (Configuration)
	 * args[2] is the HTableDescriptor (HTableDescriptor) 
	 * args[3] is the tableName (String) 
	 * args[4] is the startRowKey (int) 
	 * args[5] is the endRowKey (int) 
	 * args[6] is the splitNum (int)
	 */

	/** hbase_createTable
	 * @param hdfsSite_PathStr
	 * @param conf
	 * @param htd
	 * @param tableName
	 * @param int StartRowKey
	 * @param int EndRowKey
	 * @param splitNum
	 * @throws IOException
	 */
	public static void hbase_createTable(String hdfsSite_PathStr,
			Configuration conf, HTableDescriptor htd, String tableName,
			int StartRowKey, int EndRowKey, int splitNum) throws IOException {

//		String StartRowKey_str = String.valueOf(StartRowKey);
		String EndRowKey_str = String.valueOf(EndRowKey);
		int RowKeyLength = EndRowKey_str.length();
		String StartRowKey_str = intToString(StartRowKey);

		// Configuration conf = HBaseConfiguration.create();
		conf.addResource(new Path(hdfsSite_PathStr));
		// conf.addResource("/usr/local/hbase/conf/hdfs-site.xml");
		HBaseAdmin admin = new HBaseAdmin(conf);
		// String tableName = "tableName";
		String familyName = "colFamily";
		// HTableDescriptor htd = new HTableDescriptor(tableName);
		HColumnDescriptor hdc = new HColumnDescriptor(familyName);
		htd.addFamily(hdc);
		long before = System.currentTimeMillis();
		// admin.createTable(htd,splits);

		// i is the row key , split to 15 , row key range: 000000~499999
		admin.createTable(htd, Bytes.toBytes(StartRowKey_str),
				Bytes.toBytes(EndRowKey_str), splitNum);

		long after = System.currentTimeMillis();
		System.out.println("[Time] create table time :  " + (after - before));

	}
	
	
	/** hbase_createTable
	 * @param hdfsSite_PathStr
	 * @param conf
	 * @param htd
	 * @param tableName
	 * @param Str StartRowKey
	 * @param Str EndRowKey
	 * @param splitNum
	 * @throws IOException
	 */
	public static void hbase_createTable(String hdfsSite_PathStr,
			Configuration conf, HTableDescriptor htd, String tableName,
			String StartRowKey, String EndRowKey, int splitNum) throws IOException {

//		String StartRowKey_str = String.valueOf(StartRowKey);
		String EndRowKey_str = EndRowKey;
		int RowKeyLength = EndRowKey_str.length();
		String StartRowKey_str = StartRowKey;

		// Configuration conf = HBaseConfiguration.create();
		conf.addResource(new Path(hdfsSite_PathStr));
		// conf.addResource("/usr/local/hbase/conf/hdfs-site.xml");
		HBaseAdmin admin = new HBaseAdmin(conf);
		// String tableName = "tableName";
		String familyName = "colFamily";
		// HTableDescriptor htd = new HTableDescriptor(tableName);
		HColumnDescriptor hdc = new HColumnDescriptor(familyName);
		htd.addFamily(hdc);
		long before = System.currentTimeMillis();
		// admin.createTable(htd,splits);

		// i is the row key , split to 15 , row key range: 000000~499999
		admin.createTable(htd, Bytes.toBytes(StartRowKey_str),
				Bytes.toBytes(EndRowKey_str), splitNum);

		long after = System.currentTimeMillis();
		System.out.println("[Time] create table time :  " + (after - before));

	}
	
	/** hbase_createTable
	 * @param hdfsSite_PathStr
	 * @param conf
	 * @param htd
	 * @param tableName
	 * @throws IOException
	 */
	public static void hbase_createTable(String hdfsSite_PathStr,
			Configuration conf, HTableDescriptor htd, String tableName) throws IOException {


		// Configuration conf = HBaseConfiguration.create();
		conf.addResource(new Path(hdfsSite_PathStr));
		// conf.addResource("/usr/local/hbase/conf/hdfs-site.xml");
		HBaseAdmin admin = new HBaseAdmin(conf);
		// String tableName = "tableName";
		String familyName = "colFamily";
		// HTableDescriptor htd = new HTableDescriptor(tableName);
		HColumnDescriptor hdc = new HColumnDescriptor(familyName);
		htd.addFamily(hdc);
		long before = System.currentTimeMillis();
		// admin.createTable(htd,splits);

		admin.createTable(htd);

		long after = System.currentTimeMillis();
		System.out.println("[Time] create table time :  " + (after - before));

	}

	/**
	 * @param x
	 * @return
	 */
	static public String intToString(int x) {
		String result = String.valueOf(x);
		int size = result.length();
		while (size < 6) {
			size++;
			result = "0" + result;
		}
		return result;
	}
	
	/** Expand String to length
	 * @param str
	 * @param length
	 * @return str
	 */
	static public String ExpandString(String str, int length) {
		int size = str.length();
		while (size < length) {
			size++;
			str = "0" + str;
		}
		return str;
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		System.out.println("[Test util] hbase_createTable ");

		/*
		 * this is the util class create the hbase table args[0] is the
		 * hdfsSite_PathStr (String) args[1] is the Configuration
		 * (Configuration) args[2] is the HTableDescriptor (HTableDescriptor)
		 * args[3] is the tableName (String) args[4] is the startRowKey (int)
		 * args[5] is the endRowKey (int) args[6] is the splitNum (int)
		 */

		System.out.print("/*\n" + " * this is the util class\n"
				+ " * create the hbase table\n"
				+ " * args[0] is the hdfsSite_PathStr 	(String)\n"
				+ " * args    is the Configuration 		(Configuration)\n"
				+ " * args    is the HTableDescriptor 		(HTableDescriptor)\n"
				+ " * args[1] is the startRowKey		(int)\n"
				+ " * args[2] is the startRowKey		(int)\n"
				+ " * args[3] is the endRowKey		(int)\n"
				+ " * args[4] is the splitNum 		(int)\n" + " * \n"
				+ " * args[5] tablename for test    \n" + " */\n");

		Configuration conf = HBaseConfiguration.create();
		HTableDescriptor htd = new HTableDescriptor(args[5]);

		try {
			hbase_createTable(args[0], conf, htd, args[1],
					Integer.parseInt(args[2]), Integer.parseInt(args[3]),
					Integer.parseInt(args[4]));
		} catch (NumberFormatException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
