package javacsv_operation;

import java.nio.charset.Charset;
import java.util.ArrayList;

import org.apache.hadoop.hbase.util.Bytes;

import com.csvreader.CsvReader;

public class Javacsv_util {
	public static ArrayList<String[]> readCsv(String csvFilePath) {

		try {

			ArrayList<String[]> csvList = new ArrayList<String[]>(); // 用来保存数据
			// String csvFilePath = "/home/fsc/ubuntu_exp/0.csv";
			CsvReader reader = new CsvReader(csvFilePath, ',',
					Charset.forName("SJIS")); // 一般用这编码读就可以了

			reader.readHeaders(); // 跳过表头 如果需要表头的话，不要写这句。

			while (reader.readRecord()) { // 逐行读入除表头的数据
				csvList.add(reader.getValues());
			}
			reader.close();

			for (int row = 0; row < csvList.size(); row++) {

				String cell = csvList.get(row)[3]; // 取得第row行第0列的数据
//				System.out.println(row + "  " + cell);

			}

			return csvList;

		} catch (Exception ex) {
			System.out.println(ex);
		}
		return null;
	}

	public static ArrayList<byte[][]> ArrayList_String2byte(
			ArrayList<String[]> Str) {

		ArrayList<byte[][]> byte_list = new ArrayList<byte[][]>();
		byte[][] byte_list_tmp = new byte[13][];

		for (int row = 0; row < Str.size(); row++) {

			for (int col = 0; col < 13; col++) {
				byte[] byte_tmp = Bytes.toBytes(Str.get(row)[col]);
				byte_list_tmp[col] = byte_tmp;
			}
			byte_list.add(byte_list_tmp);
		}
		return byte_list;
	}
}
