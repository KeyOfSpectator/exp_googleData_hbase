package hbase_thoughput;

import java.nio.charset.Charset;
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

import com.csvreader.CsvReader;

/*
 * $java -jar thoughput_hbase_2.jar XXX XXX XXX
 * 
 * optimize the read to avoid heap overflow
 * 
 * This is the demo to write google monitor data
 * thoughput method 
 * 
 * author: fsc
 * 2015-1-28
 * 
 * args	
 * args[0] :  csvFilePath 		  	exp:"/home/fsc/ubuntu_exp/0.csv";
 * args[1] :  hbase-site_PathStr  	exp:"/home/fsc/hbase/hbase-0.98.9-hadoop2/conf/hbase-site.xml";
 * args[2] :  tableName				exp:"google_exp_57M_singleMachine";
 * 
 */

public class Exp_thoughput_bigData {

	static String familyName = "imageFamily";
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
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
//		config
		
		String csvFilePath = args[0];
		String site_PathStr = args[1];
		String tableName   = args[2];
		
		Configuration conf = HBaseConfiguration.create();
		
		HTableDescriptor htd = new HTableDescriptor(tableName);
		
		hbase_create_table( site_PathStr , tableName , conf , htd);
		
		try {
			// String csvFilePath = "/home/fsc/ubuntu_exp/0.csv";
			CsvReader reader = new CsvReader(csvFilePath, ',' , Charset.forName("SJIS")); // 一般用这编码读就可以了
			
			reader.readHeaders(); // 跳过表头 如果需要表头的话，不要写这句。
			
			int i = 0;
			long total_write_time = 0;
			
			HTable table = new HTable(conf, htd.getName());
			table.setAutoFlush(false);
			// table.setWriteBufferSize(209715200);
			System.out.println("[Info] WriteBufferSize :  " + table.getWriteBufferSize());
			
			
			while (reader.readRecord()) { // 逐行读入除表头的数据
//				csvList.add(Bytes.toBytes( reader.getValues());
				
//				data_line:
//				Bytes.toBytes( reader.getValues()
				
//				hbase write
				
				long begin = System.currentTimeMillis();
				
//				for (int i = 0; i < 10000000; i++) {
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
					p1.add(Bytes.toBytes(familyName), Bytes.toBytes(col0), Bytes.toBytes( reader.getValues()[0] ) );
					p1.add(Bytes.toBytes(familyName), Bytes.toBytes(col1), Bytes.toBytes( reader.getValues()[1] ) );
					p1.add(Bytes.toBytes(familyName), Bytes.toBytes(col2), Bytes.toBytes( reader.getValues()[2] ) );
					p1.add(Bytes.toBytes(familyName), Bytes.toBytes(col3), Bytes.toBytes( reader.getValues()[3] ) );
					p1.add(Bytes.toBytes(familyName), Bytes.toBytes(col4), Bytes.toBytes( reader.getValues()[4] ) );
					p1.add(Bytes.toBytes(familyName), Bytes.toBytes(col5), Bytes.toBytes( reader.getValues()[5] ) );
					p1.add(Bytes.toBytes(familyName), Bytes.toBytes(col6), Bytes.toBytes( reader.getValues()[6] ) );
					p1.add(Bytes.toBytes(familyName), Bytes.toBytes(col7), Bytes.toBytes( reader.getValues()[7] ) );
					p1.add(Bytes.toBytes(familyName), Bytes.toBytes(col8), Bytes.toBytes( reader.getValues()[8] ) );
					p1.add(Bytes.toBytes(familyName), Bytes.toBytes(col9), Bytes.toBytes( reader.getValues()[9] ) );
					p1.add(Bytes.toBytes(familyName), Bytes.toBytes(col10), Bytes.toBytes( reader.getValues()[10] ) );
					p1.add(Bytes.toBytes(familyName), Bytes.toBytes(col11), Bytes.toBytes( reader.getValues()[11] ) );
					p1.add(Bytes.toBytes(familyName), Bytes.toBytes(col12), Bytes.toBytes( reader.getValues()[12] ) );
					
					table.put(p1);
					
//					System.out.println(i);
					
//				}//end for
				long end = System.currentTimeMillis();
				total_write_time += (end - begin);
				i++;
			}//end while
			table.flushCommits();
			System.out.println("[Time] HBase write time : " + total_write_time);
			reader.close();
		}
		catch (Exception ex) {
			System.out.println(ex);
		}
	}

	private static void hbase_create_table(String hdfsSite_PathStr,
			String tableName , Configuration conf , HTableDescriptor htd) {
		// TODO Auto-generated method stub

		try {

//			Configuration conf = HBaseConfiguration.create();
			conf.addResource(new Path(hdfsSite_PathStr));
			// conf.addResource("/usr/local/hbase/conf/hdfs-site.xml");
			HBaseAdmin admin = new HBaseAdmin(conf);
			// String tableName = "tableName";

//			HTableDescriptor htd = new HTableDescriptor(tableName);
			HColumnDescriptor hdc = new HColumnDescriptor(familyName);
			htd.addFamily(hdc);
			long before = System.currentTimeMillis();
			// admin.createTable(htd,splits);

			/*
			 * public void createTable(HTableDescriptor desc, byte[] startKey,
			 * byte[] endKey, int numRegions) throws IOException
			 */

			// i is the row key , split to 15 , row key range: 00000000~99999999
			admin.createTable(htd, Bytes.toBytes("00000000"),
					Bytes.toBytes("99999999"), 15);

			long after = System.currentTimeMillis();
			System.out.println("[Time] create table time :  "
					+ (after - before));

		} catch (Exception ex) {
			System.out.println(ex);
		}
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

}
