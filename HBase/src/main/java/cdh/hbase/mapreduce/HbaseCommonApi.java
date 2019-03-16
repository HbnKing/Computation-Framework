package cdh.hbase.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;


/**
 * hbase 增删改查
 * @author wangheng
 *
 */
public class HbaseCommonApi {
	
	private String tableName = null;
	private HTable table = null;
	private ResultScanner rs = null;
	public HbaseCommonApi(){}
	public HbaseCommonApi(String tableName){
		
		this.setTableName(tableName);
	}

	//本地测试需要配置hosts 映射
	private static Configuration conf;
	static {
		conf = HBaseConfiguration.create(); // 第一步
		//conf.set("hbase.zookeeper.quorum", "192.168.0.30,192.168.0.31,192.168.0.32");
		//conf.set("hbase.zookeeper.property.clientPort", "2181");
		//conf.set("hbase.master", "192.168.0.30:60000,192.168.0.31:60000,192.168.0.32:60000" );
	}
	
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub	
		HbaseCommonApi ht = new HbaseCommonApi();
		
		
		 /*Connection connection = ConnectionFactory.createConnection(conf);
	     //建立表的连接
	     Table table11 = connection.getTable(TableName.valueOf("testtable"));*/
		try {
			String tableName = "member1";
			String rowkey = "007";
			//ht.createTable("member1" ,new ArrayList<String>(Arrays.asList("lines","line3")) );
			//ht.getAllTableNames();
			//ht.insertDataByPut("member1","001" ,"lines" ,"linestmp" ,"truetmp");
			//ht.insertDataByPut("member1","003" ,"lines" ,"linestmp" ,"falsetmp");
			//ht.insertDataByPut("member1","004" ,"lines" ,"linestmp" ,"falsetmp");
			//ht.insertDataByPut("member1","005" ,"lines" ,"linestmp" ,"falsetmp");
			//ht.insertDataByPut("member1","006" ,"lines" ,"linestmp" ,"falsetmp");
			ht.insertDataByPut("member1","08" ,"lines" ,"linestmp" ,"falsetmp");
			ht.insertDataByPut("member1","06" ,"lines" ,"linestmp" ,"falsetmp");
			ht.insertDataByPut("member1","03" ,"lines" ,"linestmp" ,"falsetmp");
			ht.insertDataByPut("member1","05" ,"lines" ,"linestmp" ,"falsetmp");
			//ht.QueryByGet("member1" ,"001");
			System.out.println(ht.QueryByGet(tableName, rowkey).isEmpty());
			System.out.println(ht.QueryByGet(tableName, rowkey).list());
			System.out.println("___________________");
			
			
			for (Result r : ht.QueryByScan(tableName ,"lines" ,"linestmp") ){
				System.out.println("测试结果 " + new String(r.getRow()));
			}
			//ht.deleteDataByRowkey(tableName, rowkey);
			for (Result r : ht.QueryByScan(tableName ) ){
				System.out.println("测试结果2 " + new String(r.getRow()));
			}
			
			
			for (Result r : ht.ScanPrefixByRowKey(tableName, "00")){
				System.out.println("测试结果3 " + new String(r.getRow()));
			}
			ht.cleanup();
			
			for (Result r :ht.ScanPrefixByRowKeyAndLimit(tableName, "0", 2) ){
				System.out.println("测试结果3 " + new String(r.getRow()));
			}
			System.out.println("**********");
			for (Result r :ht.ScanPrefixByRowKeyAndLimit(tableName, "0", 2) ){
				System.out.println("测试结果4" + new String(r.getRow()));
			}
			
			for(Result r :ht.scanByStartAndStopRow(tableName,"0" ,"06")){
				System.out.println("测试结果5 " + new String(r.getRow()));
			}
			
			/*System.out.println("00".compareTo("01"));
			System.out.println("00".compareTo("001"));
			System.out.println("00".compareTo("0"));
			System.out.println("00".compareTo("00"));*/
			// QueryByScan("member1");
			 //deleteData("member");
			//ht.deleteTable("wordcount2");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	/**
	 * 1创建表通过HBaseAdmin对象操作
	 * 
	 * @param tableName
	 * @throws IOException
	 * @throws ZooKeeperConnectionException
	 * @throws MasterNotRunningException
	 */
	public void createTable(String tableName ,List<String> columnFamilyNames)
			throws MasterNotRunningException, ZooKeeperConnectionException,
			IOException {
		HBaseAdmin hBaseAdmin = new HBaseAdmin(conf);// 创建HBaseAdmin对象
		if (hBaseAdmin.tableExists(tableName)) {
			deleteTable(tableName);
		}
		HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
		// 添加Family
		
		for(String str : columnFamilyNames){
			tableDescriptor.addFamily(new HColumnDescriptor(str));
		}
		hBaseAdmin.createTable(tableDescriptor); // 创建表
		hBaseAdmin.close();// 释放资源
	}
	
	/**
	 * 2查询所有表名
	 * @return
	 * @throws IOException 
	 * @throws ZooKeeperConnectionException 
	 * @throws MasterNotRunningException 
	 * @throws Exception
	 */
	public List<String> getAllTableNames() throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
		ArrayList<String> tables = new ArrayList<String>();
		HBaseAdmin admin = new HBaseAdmin(conf);
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
	 * 3插入一条记录
	 * 
	 * @param tableName
	 * @throws IOException
	 */
	public void insertDataByPut(String tableName, String rowkey,String familyName,String columnName, String value) throws IOException {
		HTable table = new HTable(conf, tableName);// 第二步
		
		Put put1 = new Put(getBytes(rowkey));
		put1.add(getBytes(familyName), getBytes(columnName), getBytes(value));

		table.put(put1);// 第三步
		table.close();// 释放资源

	}
	
	
	/**
	 * 4查询一条记录
	 * 
	 * @param tableName
	 * @return 
	 * @throws IOException
	 */
	public Result QueryByGet(String tableName,String rowkey) throws IOException {
		HTable table = new HTable(conf, tableName);// 第二步
		Get get = new Get(getBytes(rowkey));// // 根据rowkey查询
		Result r = table.get(get);
		/*System.out.println("获得到rowkey:" + new String(r.getRow()));
		for (KeyValue keyValue : r.raw()) {
			
			System.out.println("列簇：" + new String(keyValue.getFamily())
					+ "====列" + new String(keyValue.getQualifier()) + "====值:"
					+ new String(keyValue.getValue()));
			
		}*/
		
		table.close();
		return r;

	}
	/**
	 * 5扫描某一列的所有值
	 * 
	 * @param tableName
	 * @throws IOException
	 */
	public ResultScanner QueryByScan(String tableName ,String columnFamily ,String column) throws IOException {
		table = new HTable(conf, tableName);// 第二步
		Scan scan = new Scan();
		scan.addColumn(getBytes(columnFamily), getBytes(column));
		rs = table.getScanner(scan);
		/**
		for (Result r : scanner) {
			System.out.println("获得到rowkey:" + new String(r.getRow()));
			for (KeyValue kv : r.raw()) {
				System.out.println("列簇：" + new String(kv.getFamily()) + "====列"
						+ new String(kv.getQualifier()) + "====值:"
						+ new String(kv.getValue()));
			}
		} */
		//scanner.close();// 释放资源

		//table.close();// 释放资源
		return rs;
	}
	/**
	 * 全表扫描
	 * @param tableName
	 * @return
	 * @throws IOException
	 */
	public ResultScanner QueryByScan(String tableName) throws IOException{
		table = new HTable(conf, tableName);// 第二步
		Scan scan = new Scan();
		rs = table.getScanner(scan);
		
		return rs;
		
	}
	


	/* 转换byte数组 */
	private byte[] getBytes(String str) {
		if (str == null)
			str = "";
		return Bytes.toBytes(str);
	}
	
	/**
	 * 删除表操作
	 * @param tableName
	 * @throws IOException
	 */
	public void deleteTable(String tableName) throws IOException {
		HBaseAdmin hBaseAdmin = new HBaseAdmin(conf);// 创建HBaseAdmin对象
		if(hBaseAdmin.tableExists(tableName)){
			hBaseAdmin.disableTable(tableName);
			hBaseAdmin.deleteTable(tableName);
		}	
		
	}
	/**
	 * 根据单个rowkey 删除数据
	 * @param tableName
	 * @param rowkey
	 * @throws IOException
	 */
	public void deleteDataByRowkey(String tableName, String rowkey) throws IOException {
		HTable table = new HTable(conf, tableName);// 第二步
		Delete delete = new Delete(getBytes(rowkey));
		table.delete(delete);
		table.close();// 释放资源

	}
	/**
	 * 多条数据删除
	 * @param tableName
	 * @param rowkeys
	 * @throws IOException
	 */
	public void deleteDataByList(String tableName, List<String> rowkeys) throws IOException  {
		HTable table = new HTable(conf, tableName);// 第二步
		List<Delete> list=new ArrayList<Delete>();
		for(String rowkey : rowkeys){
			Delete delete = new Delete(getBytes(rowkey));
			list.add(delete);
		}
		table.delete(list);
		table.close();// 释放资源
	}
	
	
	/**
	 * 根据主键前缀查询
	 * @param tableName
	 * @param rowKey
	 * @return 
	 * @throws Exception 
	 */
	public ResultScanner ScanPrefixByRowKey(String tableName ,String rowKeyPre) throws Exception{
		table=new HTable(conf, tableName);
		Scan scan=new Scan();
		scan.setFilter(new PrefixFilter(Bytes.toBytes(rowKeyPre)));
		rs=table.getScanner(scan);
		/*for(Result r:rs){
			printRecoder(r);//打印记录
		}
		table.close();//释放资源
		
		 */	
		return rs;
	}
	/**
	 * 查看某个表下的所有数据
	 * 
	 * @param tableName 表名
	 * @param rowKey 行健扫描
	 * @param limit  限制返回数据量
	 * */
	public ResultScanner ScanPrefixByRowKeyAndLimit(String tableName,String rowKey,long limit)throws Exception{
		table=new HTable(conf, tableName);
		Scan scan=new Scan();
		scan.setFilter(new PrefixFilter(Bytes.toBytes(rowKey)));
		scan.setFilter(new  PageFilter(limit));
		rs=table.getScanner(scan);
		/*for(Result r:rs){
			printRecoder(r);//打印记录
		}
		table.close();//释放资源
		*/
		
		return rs;
	}
	
	/**
	 * 根据rowkey扫描一段范围
	 * @param tableName 表名
	 * @param startRow 开始的行健
	 * @param stopRow  结束的行健
	 * @return 
	 * **/
	public  ResultScanner scanByStartAndStopRow(String tableName,String startRow,String stopRow)throws Exception{
		table=new HTable(conf, tableName);
		Scan scan=new Scan();
		scan.setStartRow(Bytes.toBytes(startRow));
		scan.setStopRow(Bytes.toBytes(stopRow));
		rs=table.getScanner(scan);
		/*for(Result r:rs){
			printRecoder(r);
		}
		table.close();//释放资源
		
		 */
		
		return rs;
		
		}
	
	
	
	/**
	 *做一些清理工作
	 */
	public void cleanup(){
		//Htable  , scann  result ,resultscanner , 
		resultScannerClose(rs);
		htableClose(table);
		
	}
	
	/**
	 * 释放ResultScanner 资源
	 * @param rs
	 */
	private void resultScannerClose(ResultScanner rs){
		if(rs != null){
			rs.close();
		}
		rs = null;
	}
	/**
	 * 关闭 释放 Htable
	 * @param table
	 */
	private void htableClose(HTable table){
		if(table != null){
			try {
				table.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		table = null ;
	}
	
	
	
	
	
	
	/**
	 * 打印一条记录的详情
	 * 
	 * */
	public  void printRecoder(Result result){
		
		for(Cell cell:result.rawCells()){
			System.out.print("行健: "+new String(CellUtil.cloneRow(cell)));
			System.out.print("列簇: "+new String(CellUtil.cloneFamily(cell)));
			System.out.print(" 列: "+new String(CellUtil.cloneQualifier(cell)));
			System.out.print(" 值: "+new String(CellUtil.cloneValue(cell)));
			System.out.println("时间戳: "+cell.getTimestamp());	
		}
	}
	
	
	public String getTableName() {
		return tableName;
	}
	
	
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
}
