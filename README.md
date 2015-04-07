## APM数据库实验 hbase 写入thoughput测量 以及查询性能测量

* 运行之前复制hbase的jar包和项目文件extend_lib中的jar包到
* ${CLASSPATH}/jre/lib/ext/

## hbase 50M 50W行数据 测thoughput
* Exp_thoughput_50WLine_DynamicThreads.java
* 打包为  jar/thoughput50WL_DT_Nologs.jar

/*
 * $java -jar thoughput50WL_DT_Nologs.jar XXX XXX XXX XXX XXX
 * 
 * This is the demo to write google monitor data
 * thoughput method 
 * 
 * author: fsc
 * 2015-1-27
 * 
 * args	
 * args[0] :  csvFolderPath 		  	exp:"/home/ubuntu/data/output${threads}/";
 * args[1] :  hbase-site_PathStr  		exp:"/home/fsc/hbase/hbase-0.98.9-hadoop2/conf/hbase-site.xml";
 * args[2] :  threadNum					exp:"${threads}";
 * args[3] :  SplitNum_Str				exp:"${split}";
 * args[4] :  tableName					exp:"${threads}T_${split}S_NPR_shellEXE2_${test_loop}";
 *
 * exp: java -jar /home/ubuntu/git_projects/exp_googleData_hbase/jar/thoughput50WL_DT_Nologs.jar /home/ubuntu/data/output${threads}/ /home/ubuntu/hbase/hbase-0.98.9-hadoop2/conf/hbase-site.xml ${threads} ${split}  ${threads}T_${split}S_NPR_shellEXE2_${test_loop} >> /home/ubuntu/shell/output/logs
 */

	
## hbase 查询
* hbase_query.java
	
## hbase 分裂表写入 测thoughput
* 分裂为两个表 XXX_1 XXX_2
* Exp_thoughput_split2table_DynamicThreads_50WLine.java
* 打包为 jar/