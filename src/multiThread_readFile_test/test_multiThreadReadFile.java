package multiThread_readFile_test;

import java.util.Vector;

import thread.OptThread_DynamicNum;
import thread.OptThread_testReadFile;

public class test_multiThreadReadFile {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		System.out.print("/* \n"
				+ " * Exp for Multi threads read file\n"
				+ " * Whole Data is 50WLine\n"
				+ " * "
				+ " * param:\n"
				+ " * args[0] : csvFolderPath\n"
				+ " * args[1] : thread Num\n"
				+ " * /\n");
		
		final String csvFolderPath = args[0];
		final String threadNum = args[1];
		final int threadCount = Integer.parseInt(threadNum);
		final int opcount = 500000/threadCount;
		
		Vector<Thread> allthreads = new Vector<Thread>();

		for (int i = 0; i < threadCount; i++) {
			/*
			 * OptThread_DynamicNum(int threadId ,int threadNum, String FolderPath , String site_PathStr
			 * , String tableName)
			 */
			Thread t = new OptThread_testReadFile(i,threadCount, csvFolderPath);
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
		System.out.println("Total read Time: "
				+ (double) (en - st)
				+ " ms");
		System.out.println("Throughput: "
						+ ((1000.0) * (((double) (opcount * threadCount)) / ((double) (en - st))))
						+ " ops/sec");
		
	}
}
