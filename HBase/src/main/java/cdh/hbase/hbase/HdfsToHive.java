package cdh.hbase.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.*;

public class HdfsToHive  {
	
	private static String driverName = "org.apache.hive.jdbc.HiveDriver";
	
	private static Connection con = null;
	private static Statement stmt = null;
	private static ResultSet res = null;
	
	private static String hiveHost = null;
	private static String hiveDB = null;
	private static String hiveUser = null;
	private static String hivePassword = null;
	private static String hiveTable = null;
	private static String hdfsUri = null;
	private static String hdfsDir = null;
	private static String hadoopUser = null;
	private static String hbaseHost = null;
	private static String hbasePort = null;
	private static Boolean isDebug = false;
	
	private static String hiveDataFile = null;
	
	private static Configuration hdfsConf = null;
	private static FileSystem    hadoopFS = null;
	
	public static void main(String[] args) {
		if (args.length < 9) {
            useage();
            System.exit(0);
    	}
		
		Map<String, String> user = new HashMap<String, String>();
    	user = System.getenv();
    	if (user.get("HADOOP_USER_NAME") == null) {
    		System.out.println("请设定hadoop的启动的用户名，环境变量名称：HADOOP_USER_NAME，对应的值是hadoop的启动的用户名");
    		System.exit(0);
    	} else {
    		hadoopUser = user.get("HADOOP_USER_NAME");
    	}
    	
    	init(args);
    	
    	Calendar calendar = java.util.Calendar.getInstance();
    	calendar.roll(java.util.Calendar.DAY_OF_YEAR, -1);
    	//昨天生成好的hdfs上的数据
    	hiveDataFile = "/" + hdfsDir + "/" + (new SimpleDateFormat("yyyy-MM-dd").format(calendar.getTime())) + ".txt";
    	
		run();
		System.out.println("run finish");
	}

	@SuppressWarnings("deprecation")
	public static void run() {
		int userid = 0;
		//向hbase进行批量插入的记录数
		int comitNum = 1000;
		
		ArrayList<UserTags> list = new ArrayList<UserTags>();
		
		List<Put> puts = new ArrayList<Put>();
		
		getConnection();
		
		execSql("create table if not exists " + hiveTable  + " (tagid int, ctime int, userid int, usertype int, rank int, decay FLOAT)  row format delimited fields terminated by '\\t'");
		
		if (!fileExists(hiveDataFile)) {
    		debug("HDFS FILE: " + hiveDataFile + " NOT FIND, SO Ignore load data to hive");
    	} else {
    		execSql("load data  inpath '" + hiveDataFile + "' into table "+ hiveTable);
    	}
		
		//生成用户的权重记录表排名前10的记录，写入到hbase中
		createHbaseTable("user_tags", "info");
		
		
		try {
			ObjectMapper mapper = new ObjectMapper();
			
			res = stmt.executeQuery("SELECT tagid, userid,  round(sum(rank * pow(decay, round((unix_timestamp() - ctime) / 86400))), 2) as realrank from usertags where usertype=1 and rank>0 group by userid,tagid order by userid asc, realrank desc");
			debug("insert data from hive to hbase");
			while (res.next()) {
				if (userid == 0) {
					userid = res.getInt(2);
				}
				
				//判断是否需要进行批量插入操作
				if (puts.size() >= comitNum) {
					bathWriteData(puts, "user_tags", "info");
					puts.clear();
				}
				
				if (userid != res.getInt(2)) {
					try {
						String json = mapper.writeValueAsString(list);
						list.clear();
						
						StringBuffer sb = new StringBuffer();
			            sb.append((new Integer(userid)).toString());
			             
			             byte[] rowkey = Bytes.toBytes(sb.reverse().toString());
			             byte[] value = Bytes.toBytes(json);
			             Put put = new Put(rowkey);
			             
			             put.add(Bytes.toBytes("info"), null, value);
			             puts.add(put);
						 userid = res.getInt(2);
						 
						 debug("user ID: " + userid + ", tag ID: " + res.getInt(1) + " finish insert to put list");
						 
					} catch (JsonGenerationException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (JsonMappingException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				
				UserTags usertag = new UserTags();
				usertag.setTagid(res.getInt(1));
				usertag.setUid(res.getInt(2));
				usertag.setRealrank(res.getFloat(3));
				list.add(usertag);
	        }
			
			//循环之外，进行最后一次的插入
			if (list.size() > 0) {
				String json = null;
				try {
					json = mapper.writeValueAsString(list);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				list.clear();
				
				StringBuffer sb = new StringBuffer();
	            sb.append((new Integer(userid)).toString());
	             
	            byte[] rowkey = Bytes.toBytes(sb.reverse().toString());
	            byte[] value = Bytes.toBytes(json);
	            Put put = new Put(rowkey);
	             
	            put.add(Bytes.toBytes("info"), null, value);
	            puts.add(put);
	            
	            bathWriteData(puts, "user_tags", "info");
				puts.clear();
			}
			
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		destroy();
	}
	
	private static void createHbaseTable(String hbase_table, String hbaseColumn) {
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", hbaseHost);
        config.set("hbase.zookeeper.property.clientPort", hbasePort);

        try {
        	org.apache.hadoop.hbase.client.Connection connection = ConnectionFactory.createConnection(config);
             Admin admin =  connection.getAdmin();
             HTableDescriptor hbaseTable = new HTableDescriptor(TableName.valueOf(hbase_table));
             hbaseTable.addFamily(new HColumnDescriptor(hbaseColumn));
             if (!admin.tableExists(TableName.valueOf(hbase_table))) {
            	 admin.createTable(hbaseTable);
             }
             admin.close();
             connection.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
	
	private static void bathWriteData(List<Put> puts, String hbase_table, String hbaseColumn) {
		 Configuration config = HBaseConfiguration.create();
		 config.set("hbase.zookeeper.quorum", hbaseHost);
		 config.set("hbase.zookeeper.property.clientPort", hbasePort);
		 try {
			 debug("execute hbase bath insert");
			 org.apache.hadoop.hbase.client.Connection connection = ConnectionFactory.createConnection(config);
             Admin admin =  connection.getAdmin();
             Table table =  connection.getTable(TableName.valueOf(hbase_table));
             
             table.put(puts);
             ((HTable) table).flushCommits();
             
             admin.close();
             connection.close();
			 
		 } catch (Exception e) {
			 e.printStackTrace();
		 }
	}

	private static boolean fileExists(String file) {
		boolean ret = false;
		
		hdfsConf = new Configuration();
    	hdfsConf.set("fs.defaultFS", hdfsUri);
    	
		try {
			hadoopFS = FileSystem.get(hdfsConf);
			ret = hadoopFS.exists(new Path(file));
			hadoopFS.close();
    	} catch (IOException e) {
			e.printStackTrace();
		}
		return ret;
	}
	
	private static void execSql(String sql) {
		try {
			debug("Running hive SQL: " + sql);
			stmt.execute(sql);
		} catch (Exception e) {
			e.printStackTrace();
			debug("error in execute hive sql: " + sql);
		}
	}
	
	private static void init(String[] args) {
		hiveHost = args[0];
		hiveDB = args[1];
		hiveUser = args[2];
		hivePassword = args[3];
		hiveTable = args[4];
		hdfsUri = args[5];
    	hdfsDir = args[6];
    	hbaseHost = args[7];
    	hbasePort = args[8];
 
    	if (args.length > 9) {
    		if (args[9].equals("true")) {
    			isDebug = true;
    		}
    	}
 
    	debug("初始化服务参数完毕,参数信息如下");
    	debug("HIVE_HOST: " + hiveHost);
    	debug("DB_NAME: " + hiveDB);
    	debug("USER: " + hiveUser);
    	debug("PASSWORD: " + hivePassword);
    	debug("TABLE: " + hiveTable);
    	debug("HDFS_URI: " + hdfsUri);
    	debug("HDFS_DIRECTORY: " + hdfsDir);
    	debug("HBASE_HOST: " + hbaseHost);
    	debug("HBASE_PORT: " + hbasePort);
    	debug("HADOOP_USER: " + hadoopUser);
    	debug("IS_DEBUG: " + isDebug);
    }
 
    private static void debug(String str) {
    	if (isDebug) {
    		System.out.println(str);
    	}
    }
 
    private static void useage() {
        System.out.println("* hdfs数据load到hive2经过统计写入到hbase的Java工具使用说明 ");
        System.out.println("# java -jar hdfstohive.jar HIVE_HOST DB_NAME USER PASSWORD TABLE HDFS_DIRECTORY IS_DEBUG");
        System.out.println("*  参数说明:");
        System.out.println("*   HIVE_HOST       : 代表hive的主机名或IP:port，例如：namenode:10000");
        System.out.println("*   DB_NAME         : 代表hive的数据库名称，例如：default");
        System.out.println("*   USER            : 代表hive链接用户名 ，例如：hive");
        System.out.println("*   PASSWORD        : 代表 hive链接密码，例如：hive");
        System.out.println("*   TABLE           : 代表 hive的表名称，例如：usertags");
        System.out.println("*   HDFS_URI        : 代表hdfs链接uri ，例如：hdfs://namenode:9000");
        System.out.println("*   HDFS_DIRECTORY  : 代表hdfs目录名称 ，例如：usertags");
        System.out.println("*   HBASE_HOST      : 代表hbase的主机名或IP，例如：namenode,datanode1,datanode2");
        System.out.println("*   HBASE_PORT      : 代表hbase的监听端口,例如：2181");
        System.out.println("*  可选参数:");
        System.out.println("*   IS_DEBUG        : 代表是否开启调试模式，true是，false否，默认为false");
    }
    
	private static void getConnection() {
		try {
			Class.forName(driverName);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		try {
			con = DriverManager.getConnection(
					"jdbc:hive2://"+hiveHost+"/" + hiveDB, hiveUser, hivePassword);
			stmt = con.createStatement();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	private static void destroy() {
		if (null != res) {
			try {
				res.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
			res = null;
		}
		if (null != stmt) {
			try {
				stmt.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
			stmt = null;
		}
		if (null != con) {
			try {
				con.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
			con = null;
		}
	}
}

class UserTags {
    private Integer tagid;
    private Integer userid;
    private Float realrank;
    
    public Integer getTagid() {
        return tagid;
    }
    
    public void setTagid(Integer tagid) {
    	this.tagid = tagid;
    }
    
    public  Integer getUid() {
        return this.userid;
    }
    
    public void setUid(Integer uid) {
        this.userid = uid;
    }
    
    public Float getRealrank() {
        return realrank;  
    }
    
    public void setRealrank(Float realrank) {  
    	this.realrank = realrank;  
    }
}

