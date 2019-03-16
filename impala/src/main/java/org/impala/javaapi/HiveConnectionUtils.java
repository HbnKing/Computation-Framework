package org.impala.javaapi;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.SQLException;
import java.util.Properties;
 

/**
 * 2018年1月
 * @author Wangheng
 *
 */
public class HiveConnectionUtils {
	private static String driverName = null;
	private static String Url = null;
	private static String user = null;  
    private static String password = null;
	static {
	 Properties prop = new Properties(); 
	 
	 
     try { 
         prop.load(new FileInputStream("props/jdbc.properties")); 
         driverName = prop.getProperty("driverName","org.apache.hive.jdbc.HiveDriver").trim(); 
         Url = prop.getProperty("Url","jdbc:hive2://192.168.0.30:10000/default").trim();
         user = prop.getProperty("user","root").trim();
         password = prop.getProperty("password","root123").trim();
     } catch (IOException e) { 
         e.printStackTrace(); 
     }  
	}
    
	/**
	 * 
	 */
    private static Connection conn;
    /**
     * 获取连接
     * @return
     */
    public static Connection getConnnection(){
        try{
          Class.forName(driverName);
          conn = DriverManager.getConnection(Url,user,password);      
          //此处的用户名一定是有权限操作HDFS的用户，否则程序会提示"permissiondeny"异常
       }catch(ClassNotFoundException  e)  {
                   e.printStackTrace();
                   System.exit(1);
                }
         catch (SQLException  e) {
            e.printStackTrace();
        }
        return conn;
    }
    
    /**
     * 释放
     * @param conn
     * @param st
     * @param rs
     */
    public static void release(Connection conn) {
    	if (conn != null) {  
            try {  
                conn.close();  
            } catch (Exception e) {  
                e.printStackTrace();  
            }  
        }  
    	
    }
    
    public static void release(Statement st) {
    	 if (st != null) {  
             try {  
                 st.close();  
             } catch (Exception e) {  
                 e.printStackTrace();  
             }  
             st = null;  
         } 
    }
   
    public static void release(ResultSet rs) {
    	if (rs != null) {  
            try {  
                rs.close();  
            } catch (Exception e) {  
                e.printStackTrace();  
            }  
            rs = null;  
        }  
    }
    
    public static void release(Connection conn, Statement st, ResultSet rs) {  
    	release(rs);
        release(st);
        release(conn);
    }
}