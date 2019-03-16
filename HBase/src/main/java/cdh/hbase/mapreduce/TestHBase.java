package cdh.hbase.mapreduce;

import java.io.IOException;

import cdh.hbase.hbase.HbaseUtil;
/**
 * Hbase 测试wordcount  mapreduce 读写
 * @author dajiangtai
 *
 */
public class TestHBase {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		HbaseUtil hbaseUtil = new HbaseUtil();
		try {
			//创建WordCount表
			hbaseUtil.createTable("wordcount2", "content");
			hbaseUtil.deleteTable("wordcount2");
			//hbaseUtil.getALLTable();
			//insert(hbaseUtil);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void insert(HbaseUtil hbaseUtil) throws IOException{
		hbaseUtil.put("wordcount", "hbase", "content", "count", "759");
		hbaseUtil.put("wordcount", "hadoop", "content", "count", "1856");
		hbaseUtil.put("wordcount", "storm", "content", "count", "618");
		hbaseUtil.put("wordcount", "spark", "content", "count", "1024");
		hbaseUtil.put("wordcount", "hive", "content", "count", "361");
		hbaseUtil.put("wordcount", "flume", "content", "count", "127");
	}

}
