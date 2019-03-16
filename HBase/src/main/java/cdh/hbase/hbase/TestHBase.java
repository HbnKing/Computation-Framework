package cdh.hbase.hbase;

import java.io.IOException;

import cdh.hbase.util.DateUtils;




public class TestHBase {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		HbaseUtil hbaseUtil = new HbaseUtil();
		String rowkey = "18911243729"+"_"+DateUtils.getCurrent(DateUtils.DATE_PATEN_YYYYMMDDHHMMSS);
		String systaskid = "430";
		String taskname = "solr全文检索";
		String type = "11";
		String state = "1";
		hbaseUtil.put(HbaseUtil.TABLE_NAME, rowkey, HbaseUtil.COLUMNFAMILY_1, HbaseUtil.COLUMNFAMILY_1_SYSTASKID, systaskid);
		hbaseUtil.put(HbaseUtil.TABLE_NAME, rowkey, HbaseUtil.COLUMNFAMILY_1, HbaseUtil.COLUMNFAMILY_1_TASKNAME, taskname);
		hbaseUtil.put(HbaseUtil.TABLE_NAME, rowkey, HbaseUtil.COLUMNFAMILY_1, HbaseUtil.COLUMNFAMILY_1_TYPE, type);
		hbaseUtil.put(HbaseUtil.TABLE_NAME, rowkey, HbaseUtil.COLUMNFAMILY_1, HbaseUtil.COLUMNFAMILY_1_STATE, state);
	}

}
