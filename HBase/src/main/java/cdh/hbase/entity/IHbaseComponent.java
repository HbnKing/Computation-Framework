package cdh.hbase.entity;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;

/**
 * 一个hbase的 interface  
 * 统一 方法名
 * @author Wangheng
 *
 */
public interface IHbaseComponent  {
	
	void createTable(String tableName ,List<String> columnFamilyNames) throws MasterNotRunningException, ZooKeeperConnectionException, IOException;
	List<String> getAllTableNames() throws MasterNotRunningException, ZooKeeperConnectionException, IOException;
	void insertDataByPut(String tableName, String rowkey,String familyName,String columnName, String value) throws IOException;
	void deleteTable(String tableName) throws IOException;
	void deleteDataByRowkey(String tableName ,String rowkey) throws IOException;
	void deleteDataByList(String tableName ,List<String> rowkeys) throws IOException;
	void showAll(String tableName) throws IOException, Exception;

}
