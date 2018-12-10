package org.hive;


import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

import org.slf4j.Logger;  
import org.slf4j.LoggerFactory;

/**
 * 2018年1月
 * @author Wangheng
 *
 */
public class QueryHiveJdbc{
	private static final Logger logger = LoggerFactory.getLogger(QueryHiveJdbc.class);
	
	/**
	 * 执行DML
	 * @param sql
	 * @return
	 * @throws Exception
	 */
	public static ResultSet executeQuery(String sql) throws Exception{
			Connection connection =HiveConnectionUtils.getConnnection();
	        ResultSet rs = null;  
	        
	        System.out.println(sql);
	        try {
	        	Statement stmt = connection.createStatement();
	            rs=stmt.executeQuery(sql);
	           
	        }catch (Exception   e) {
	        	logger.error("sql = {}执行出错,Exception = {}", sql, e.getLocalizedMessage()); 
	            throw (e);
	        }
	        return rs;
	    }
	/**
	 * 执行DDL
	 * @param sql
	 * @return
	 * @throws Exception
	 */
	public static boolean execute(String sql) throws Exception{
		 Connection connection =HiveConnectionUtils.getConnnection();
	        PreparedStatement preState = null;  
	        ResultSet rs = null;  
	        boolean flag;
	        System.out.println(sql);
	        try {
	        	Statement stmt = connection.createStatement();
	           // rs=stmt.executeQuery(sql);
	           flag = stmt.execute(sql);
	        }catch (Exception   e) {
	        	logger.error("sql = {}执行出错,Exception = {}", sql, e.getLocalizedMessage()); 
	            throw (e);
	        }finally {  
	        	HiveConnectionUtils.release(connection,preState,rs);  
	        }  
	        return flag;
	    }
}