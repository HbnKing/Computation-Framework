package org.common;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * date 2018-04-04
 * @author Wangheng
 * 一个简单的关闭sql连接的类
 *
 */
public class SQLutil{
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
		//应该按照顺序关闭
		release(rs);
		release(st);
		release(conn);
	
	}
}
