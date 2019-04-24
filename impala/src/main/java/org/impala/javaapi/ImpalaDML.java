package org.impala.javaapi;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.common.*;

//import com.hive.jdbcutils.HiveConnectionUtils;

public class ImpalaDML
{
    static String JDBC_DRIVER = "com.cloudera.impala.jdbc41.Driver";
    static String CONNECTION_URL = "jdbc:impala://192.168.0.30:21050/default";

    public  ResultSet getResultSet(String sql){
        Connection con = null;
        ResultSet rs = null;
        PreparedStatement ps = null;
        
        try
        {
            Class.forName(JDBC_DRIVER);
            con = DriverManager.getConnection(CONNECTION_URL);
            ps = con.prepareStatement(sql);
            rs = ps.executeQuery();
            
        } catch(SQLException e){
        	e.printStackTrace();
        } catch (Exception e){
            e.printStackTrace();
        } finally {	
        	
            //关闭rs、ps和con
        	//SQLutil.release(con);
        	//SQLutil.release(ps);
        	
        }
		return rs;
    }
    
    public void release(Connection conn, Statement st, ResultSet rs ){
    	SQLutil.release(conn ,st ,rs);
    }
   
    
    
    
}