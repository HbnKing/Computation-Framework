package org.kylin.jdbc;

import java.sql.Connection;  
import java.sql.Driver;  
import java.sql.PreparedStatement;  
import java.sql.ResultSet;  
import java.util.Properties;  
   
public class AnylistWithPS {  
    public static void main(String[] args) throws Exception {  
         anylist();  
    }  
   
    public static void anylist() throws Exception {  
         Driver driver =(Driver) Class.forName("org.apache.kylin.jdbc.Driver").newInstance();  
         Properties info= new Properties();  
         info.put("user","ADMIN");  
         info.put("password","KYLIN");  
         Connection conn= driver.connect("jdbc:kylin://192.168.1.128:7070/learn_kylin",info);  
   
         PreparedStatement state = conn.prepareStatement("select * from KYLIN_CATEGORY_GROUPINGS where LEAF_CATEG_ID = ?");  
         state.setLong(1,10058);  
                    
         ResultSet resultSet = state.executeQuery();  
         while (resultSet.next()) {  
                   String col1 = resultSet.getString(1);  
                   String col2 = resultSet.getString(2);  
                   String col3 = resultSet.getString(3);  
                   System.out.println(col1+ "\t" + col2 + "\t" + col3);  
             }  
      }  
}  