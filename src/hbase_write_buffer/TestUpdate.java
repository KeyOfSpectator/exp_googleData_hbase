/*This is a demo for HDFS write test 
 * and Hbase write test write test with buffer
 * 
 */

package hbase_write_buffer;

import java.io.IOException;
import java.math.BigInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class TestUpdate {

//	hdfs write
//	abandon
	public static void testHDFS() throws IOException {
		String str = "hdfs://cloudgis4:9000/usr/tmp/";
		Path path = new Path(str);
		Configuration conf = new Configuration();
		conf.addResource(new Path("/usr/local/hadoop/conf/hdfs-site.xml"));
		FileSystem hdfs = path.getFileSystem(conf);
		hdfs.setReplication(path, (short) 4);
		FSDataOutputStream fsDataOut = hdfs.create(new Path(str + "zzz"));
		long begin = System.currentTimeMillis();
		for (int i = 0; i < 10000000; i++) {
			// byte [] kkk=new byte[10000+i/1000];
			byte[] kkk = new byte[12];
			fsDataOut.write(kkk);
			// fsDataOut.close();
			// hdfs.close();
		}
		fsDataOut.close();
		long end = System.currentTimeMillis();
		System.out.println("hdfs:" + (end - begin));
	}

	public static void testHBase() throws IOException {
		Configuration conf = HBaseConfiguration.create();
		conf.addResource(new Path(
				"/home/fsc/hbase/hbase-0.98.9-hadoop2/conf/hbase-site.xml"));
		// conf.addResource("/usr/local/hbase/conf/hdfs-site.xml");
		HBaseAdmin admin = new HBaseAdmin(conf);
		String tableName = "qq";
		String familyName = "imageFamily";
		String columnName = "imageColumn";
		HTableDescriptor htd = new HTableDescriptor(tableName);
		HColumnDescriptor hdc = new HColumnDescriptor(familyName);
		htd.addFamily(hdc);
		long before = System.currentTimeMillis();
		// admin.createTable(htd,splits);

		/*
		 * public void createTable(HTableDescriptor desc, byte[] startKey, byte[] endKey, int numRegions) throws IOException
		 */
		
		admin.createTable(htd, Bytes.toBytes("00000"),
				Bytes.toBytes("49999"), 5);
		long after = System.currentTimeMillis();
		System.out.println(after - before);
		HTable table = new HTable(conf, htd.getName());
		table.setAutoFlush(false);
		// table.setWriteBufferSize(209715200);
		System.out.println(table.getWriteBufferSize());
		long begin = System.currentTimeMillis();
		for (int i = 0; i < 50000; i++) {
			byte[] kkk = new byte[10000 + i / 1000];
			// byte [] kkk=new byte[12];
			
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
			p1.add(Bytes.toBytes(familyName), Bytes.toBytes(columnName), kkk);
			table.put(p1);
			
//			System.out.println(i);
			
		}
		long end = System.currentTimeMillis();
		table.flushCommits();
		System.out.println("HBase:" + (end - begin));
	}

	static public String intToString(int x) {
		String result = String.valueOf(x);
		int size = result.length();
		while (size < 5) {
			size++;
			result = "0" + result;
		}
		return result;
	}

	public static void main(String[] args) throws IOException {
		testHBase();
	}
}
