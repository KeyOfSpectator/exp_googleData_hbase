//package hbase_write_buffer;
//
//import java.io.IOException;
//import java.util.ArrayList;
//
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.hbase.HBaseConfiguration;
//import org.apache.hadoop.hbase.HColumnDescriptor;
//import org.apache.hadoop.hbase.HTableDescriptor;
//import org.apache.hadoop.hbase.client.HBaseAdmin;
//import org.apache.hadoop.hbase.client.HTable;
//import org.apache.hadoop.hbase.client.Put;
//import org.apache.hadoop.hbase.util.Bytes;
//
//public class Hbase_storeToTable_googleData {
//
//	public static void hbase_store(String hdfsSite_PathStr, String tableName,
//			ArrayList<byte[][]> data_list) throws IOException {
//
////		Configuration conf = HBaseConfiguration.create();
////		conf.addResource(new Path(hdfsSite_PathStr));
////		// conf.addResource("/usr/local/hbase/conf/hdfs-site.xml");
////		HBaseAdmin admin = new HBaseAdmin(conf);
////		// String tableName = "tableName";
////		String familyName = "imageFamily";
////		String col0 = "time";
////		String col1 = "missing";
////		String col2 = "job";
////		String col3 = "task";
////		String col4 = "machine";
////		String col5 = "event";
////		String col6 = "user";
////		String col7 = "scheduling";
////		String col8 = "priority";
////		String col9 = "cpu";
////		String col10 = "memory";
////		String col11 = "disk";
////		String col12 = "different";
////
////		HTableDescriptor htd = new HTableDescriptor(tableName);
////		HColumnDescriptor hdc = new HColumnDescriptor(familyName);
////		htd.addFamily(hdc);
////		long before = System.currentTimeMillis();
////		// admin.createTable(htd,splits);
////
////		/*
////		 * public void createTable(HTableDescriptor desc, byte[] startKey,
////		 * byte[] endKey, int numRegions) throws IOException
////		 */
////
////		// i is the row key , split to 15 , row key range: 000000~499999
////		admin.createTable(htd, Bytes.toBytes("000000"),
////				Bytes.toBytes("499999"), 15);
////
////		long after = System.currentTimeMillis();
////		System.out.println("[Time] create table time :  " + (after - before));
//
//		HTable table = new HTable(conf, htd.getName());
//		table.setAutoFlush(false);
//		// table.setWriteBufferSize(209715200);
//		System.out.println("[Info] WriteBufferSize :  "
//				+ table.getWriteBufferSize());
//
//		long begin = System.currentTimeMillis();
//
//		for (int i = 0; i < 50000; i++) {
//			// byte[] kkk = new byte[10000 + i / 1000];
//			// byte [] kkk=new byte[12];
//
//			/*
//			 * Put(byte[] row) Create a Put operation for the specified row.
//			 */
//			Put p1 = new Put(Bytes.toBytes(intToString(i)));
//			p1.setWriteToWAL(false);
//
//			/*
//			 * add(byte[] family, byte[] qualifier, byte[] value) Add the
//			 * specified column and value to this Put operation.
//			 */
//			p1.add(Bytes.toBytes(familyName), Bytes.toBytes(col0),
//					data_list.get(i)[0]);
//			p1.add(Bytes.toBytes(familyName), Bytes.toBytes(col1),
//					data_list.get(i)[1]);
//			p1.add(Bytes.toBytes(familyName), Bytes.toBytes(col2),
//					data_list.get(i)[2]);
//			p1.add(Bytes.toBytes(familyName), Bytes.toBytes(col3),
//					data_list.get(i)[3]);
//			p1.add(Bytes.toBytes(familyName), Bytes.toBytes(col4),
//					data_list.get(i)[4]);
//			p1.add(Bytes.toBytes(familyName), Bytes.toBytes(col5),
//					data_list.get(i)[5]);
//			p1.add(Bytes.toBytes(familyName), Bytes.toBytes(col6),
//					data_list.get(i)[6]);
//			p1.add(Bytes.toBytes(familyName), Bytes.toBytes(col7),
//					data_list.get(i)[7]);
//			p1.add(Bytes.toBytes(familyName), Bytes.toBytes(col8),
//					data_list.get(i)[8]);
//			p1.add(Bytes.toBytes(familyName), Bytes.toBytes(col9),
//					data_list.get(i)[9]);
//			p1.add(Bytes.toBytes(familyName), Bytes.toBytes(col10),
//					data_list.get(i)[10]);
//			p1.add(Bytes.toBytes(familyName), Bytes.toBytes(col11),
//					data_list.get(i)[11]);
//			p1.add(Bytes.toBytes(familyName), Bytes.toBytes(col12),
//					data_list.get(i)[12]);
//
//			table.put(p1);
//
//			// System.out.println(i);
//
//		}
//		long end = System.currentTimeMillis();
//		table.flushCommits();
//		System.out.println("[Time] HBase write time : " + (end - begin));
//	}
//
//	static public String intToString(int x) {
//		String result = String.valueOf(x);
//		int size = result.length();
//		while (size < 6) {
//			size++;
//			result = "0" + result;
//		}
//		return result;
//	}
//
//	public static void main(String[] args) {
//		// TODO Auto-generated method stub
//		System.out.println("[Test util] hbase_createTable ");
//
//		/*
//		 * this is the util class create the hbase table args[0] is the
//		 * hdfsSite_PathStr (String) args[1] is the Configuration
//		 * (Configuration) args[2] is the HTableDescriptor (HTableDescriptor)
//		 * args[3] is the tableName (String) args[4] is the startRowKey (int)
//		 * args[5] is the endRowKey (int) args[6] is the splitNum (int)
//		 */
//
//		System.out.print("/*\n" + " * this is the util class\n"
//				+ " * create the hbase table\n"
//				+ " * args[0] is the hdfsSite_PathStr 	(String)\n"
//				+ " * args    is the Configuration 		(Configuration)\n"
//				+ " * args    is the HTableDescriptor 		(HTableDescriptor)\n"
//				+ " * args[1] is the startRowKey		(int)\n"
//				+ " * args[2] is the startRowKey		(int)\n"
//				+ " * args[3] is the endRowKey		(int)\n"
//				+ " * args[4] is the splitNum 		(int)\n" + " * \n"
//				+ " * args[5] tablename for test    \n" + " */\n");
//
//		Configuration conf = HBaseConfiguration.create();
//		HTableDescriptor htd = new HTableDescriptor(args[5]);
//
//		try {
//			hbase_createTable(args[0], conf, htd, args[1],
//					Integer.parseInt(args[2]), Integer.parseInt(args[3]),
//					Integer.parseInt(args[4]));
//		} catch (NumberFormatException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//	}
//
//}
