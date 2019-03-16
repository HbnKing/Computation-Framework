package org.impala.api;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.common.SQLutil;
import org.impala.javaapi.ImpalaDML;

public class TT {
	public static void main(String[] args) throws SQLException {
		ImpalaDML ttt = new ImpalaDML();
		ResultSet rs = ttt.getResultSet("");
		 while (rs.next()){
         	
             //System.out.println(rs.getLong(1) +""+'\t' + rs.getString(2));
         	
         	System.out.println(rs.getString(2) + '\t' + rs.getString(1));
         	
             
         }
		 SQLutil.release(rs);
	}

}
