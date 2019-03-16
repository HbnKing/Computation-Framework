package cdh.hbase.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import cdh.hbase.entity.Page;
import cdh.hbase.entity.Task;
import cdh.hbase.util.DateUtils;

/**
 * HBase 工具类 
 * Created by dajiangtai on 2016-10-04
 */
public class HbaseUtil {

	/**
	 * create 'djt','info',SPLITS=>['1000000','2000000','3000000']
	 * create 'task_user_course_201703','taskinfo',SPLITS=>['130','131','136','139','150','152','186','189']
	 * 
	 */
	/**
	 * HBASE 表名称
	 */
	public static final String TABLE_NAME = "task_user_course_201703";
	/**
	 * 列簇1
	 */
	public static final String COLUMNFAMILY_1 = "taskinfo";
	/**
	 * 列簇1中的列
	 */
	public static final String COLUMNFAMILY_1_SYSTASKID = "systaskid";
	public static final String COLUMNFAMILY_1_TASKNAME = "taskname";
	public static final String COLUMNFAMILY_1_TYPE = "type";
	public static final String COLUMNFAMILY_1_STATE = "state";

	HBaseAdmin admin = null;
	Configuration conf = null;

	/**
	 * 构造函数加载配置
	 */
	public HbaseUtil() {
		conf = new Configuration();
		conf.set("hbase.zookeeper.quorum", "master,slave1,slave2");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		conf.set("hbase.master", "master:60000");
		try {
			admin = new HBaseAdmin(conf);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws Exception {
		HbaseUtil hbase = new HbaseUtil();
		// 创建一张表
		//hbase.createTable(TABLE_NAME, COLUMNFAMILY_1);
		// 查询所有表名
		//hbase.getALLTable();
		//删除表
		//hbase.deleteTable("djt");
		//删除一条记录
		//hbase.deleteOneRecord("task_user_course_" + DateUtils.getCurrent(DateUtils.YYYYMM), "18911243729_20170309132659");
		//scan 扫描18911243729 13603764689
		//hbase.getUserTaskInfoByScan("20170309", "18911243729");
		//Get 查询一条记录
		//hbase.getUserTaskInfoByRowkey("13603764689_20170309114805");
		hbase.getUserTaskInfoByScanAndFilter("20170309", "18911243729");
		
	}
	/**
	 * Get根据rowkey查询某一个记录
	 * @param rowkey
	 */
	public  void getUserTaskInfoByRowkey(String rowkey){
		HTablePool hTablePool = new HTablePool(conf, 1000);
		//查询用户当月表
		HTableInterface table = hTablePool.getTable("task_user_course_" + DateUtils.getCurrent(DateUtils.YYYYMM));
		
		try {
			Get get = new Get(rowkey.getBytes());
			Result result = table.get(get);
			if(!result.isEmpty()){
				String taskname = Bytes.toString(CellUtil.cloneValue(result.getColumnLatestCell(Bytes.toBytes("taskinfo"), Bytes.toBytes("taskname"))));
				String systaskid = Bytes.toString(CellUtil.cloneValue(result.getColumnLatestCell(Bytes.toBytes("taskinfo"), Bytes.toBytes("systaskid"))));
				String type = Bytes.toString(CellUtil.cloneValue(result.getColumnLatestCell(Bytes.toBytes("taskinfo"), Bytes.toBytes("type"))));
				String state = Bytes.toString(CellUtil.cloneValue(result.getColumnLatestCell(Bytes.toBytes("taskinfo"), Bytes.toBytes("state"))));
				System.out.println(rowkey+":"+systaskid+","+taskname+","+type+","+state);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	/**
	 * scan filter 扫描当月某用户当天所有完成任务
	 * @param day
	 * @param userId
	 * @throws Exception
	 */
	public  void getUserTaskInfoByScanAndFilter(String day, String userId) throws Exception{
		List<Task> resList = new ArrayList();
		HTablePool hTablePool = new HTablePool(conf, 1000);
		//查询用户当月表
		HTableInterface table = hTablePool.getTable("task_user_course_" + DateUtils.getCurrent(DateUtils.YYYYMM));
		//当前用户+当前时间
		String rowKey = userId + "_" + DateUtils.getCurrent(DateUtils.YYYYMMDD);
		//通过scan扫描当天所有完成任务
		Scan s = new Scan();
		FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE);
		filterList.addFilter(new SingleColumnValueFilter(
                Bytes.toBytes("taskinfo"),
                Bytes.toBytes("state"),
                CompareOp.EQUAL,
                Bytes.toBytes("1")
        ));
		s.setFilter(filterList);
		s.setStartRow(Bytes.toBytes(rowKey));
		ResultScanner rsa =table.getScanner(s);
		for (Result result : rsa) {
			String rowkey = new String(result.getRow());
			String taskname = Bytes.toString(CellUtil.cloneValue(result.getColumnLatestCell(Bytes.toBytes("taskinfo"), Bytes.toBytes("taskname"))));
			String systaskid = Bytes.toString(CellUtil.cloneValue(result.getColumnLatestCell(Bytes.toBytes("taskinfo"), Bytes.toBytes("systaskid"))));
			String type = Bytes.toString(CellUtil.cloneValue(result.getColumnLatestCell(Bytes.toBytes("taskinfo"), Bytes.toBytes("type"))));
			String state = Bytes.toString(CellUtil.cloneValue(result.getColumnLatestCell(Bytes.toBytes("taskinfo"), Bytes.toBytes("state"))));
			System.out.println(rowkey+":"+systaskid+","+taskname+","+type+","+state);
		}
	}
	/**
	 * scan 扫描当月某用户当天所有完成任务
	 * @param day
	 * @param userId
	 * @throws Exception
	 */
	public  void getUserTaskInfoByScan(String day, String userId) throws Exception{
		List<Task> resList = new ArrayList();
		HTablePool hTablePool = new HTablePool(conf, 1000);
		//查询用户当月表
		HTableInterface table = hTablePool.getTable("task_user_course_" + DateUtils.getCurrent(DateUtils.YYYYMM));
		//当前用户+当前时间
		String rowKey = userId + "_" + DateUtils.getCurrent(DateUtils.YYYYMMDD);
		//通过scan扫描当天所有完成任务
		Scan s = new Scan();
		s.setStartRow(Bytes.toBytes(rowKey));
		ResultScanner rsa =table.getScanner(s);
		for (Result result : rsa) {
			String rowkey = new String(result.getRow());
			String taskname = Bytes.toString(CellUtil.cloneValue(result.getColumnLatestCell(Bytes.toBytes("taskinfo"), Bytes.toBytes("taskname"))));
			String systaskid = Bytes.toString(CellUtil.cloneValue(result.getColumnLatestCell(Bytes.toBytes("taskinfo"), Bytes.toBytes("systaskid"))));
			String type = Bytes.toString(CellUtil.cloneValue(result.getColumnLatestCell(Bytes.toBytes("taskinfo"), Bytes.toBytes("type"))));
			String state = Bytes.toString(CellUtil.cloneValue(result.getColumnLatestCell(Bytes.toBytes("taskinfo"), Bytes.toBytes("state"))));
			System.out.println(rowkey+":"+systaskid+","+taskname+","+type+","+state);
		}
	}

	/**
	 * 删除表
	 * @param tableName
	 */
	public void deleteTable(String tableName) {
		try {
			if (admin.tableExists(tableName)) {
				admin.disableTable(tableName);
				admin.deleteTable(tableName);
				System.out.println(tableName + "表删除成功！");
			}
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println(tableName + "表删除失败！");
		}

	}

	/**
	 * 删除一条记录
	 * @param tableName
	 * @param rowKey
	 */
	public void deleteOneRecord(String tableName, String rowKey) {
		HTablePool hTablePool = new HTablePool(conf, 1000);
		HTableInterface table = hTablePool.getTable(tableName);
		Delete delete = new Delete(rowKey.getBytes());
		try {
			table.delete(delete);
			System.out.println(rowKey + "记录删除成功！");
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println(rowKey + "记录删除失败！");
		}
	}

	/**
	 * 添加一条记录
	 * @param tableName
	 * @param row
	 * @param columnFamily
	 * @param column
	 * @param data
	 * @throws IOException
	 */
	public void put(String tableName, String row, String columnFamily,
			String column, String data) throws IOException {
		HTablePool hTablePool = new HTablePool(conf, 1000);
		HTableInterface table = hTablePool.getTable(tableName);
		Put p1 = new Put(Bytes.toBytes(row));
		p1.add(Bytes.toBytes(columnFamily), Bytes.toBytes(column),
				Bytes.toBytes(data));
		table.put(p1);
		System.out.println("put'" + row + "'," + columnFamily + ":" + column
				+ "','" + data + "'");
	}

	/**
	 * 查询所有表名
	 * @return
	 * @throws Exception
	 */
	public List<String> getALLTable() throws Exception {
		ArrayList<String> tables = new ArrayList<String>();
		if (admin != null) {
			HTableDescriptor[] listTables = admin.listTables();
			if (listTables.length > 0) {
				for (HTableDescriptor tableDesc : listTables) {
					tables.add(tableDesc.getNameAsString());
					System.out.println(tableDesc.getNameAsString());
				}
			}
		}
		return tables;
	}

	/**
	 * 创建一张表
	 * 
	 * @param tableName
	 * @param column
	 * @throws Exception
	 */
	public void createTable(String tableName, String column) throws Exception {
		if (admin.tableExists(tableName)) {
			
			System.out.println(tableName + "表已经存在！");
		} else {
			HTableDescriptor tableDesc = new HTableDescriptor(tableName);
			tableDesc.addFamily(new HColumnDescriptor(column.getBytes()));
			admin.createTable(tableDesc);
			System.out.println(tableName + "表创建成功！");
		}
	}
	
}