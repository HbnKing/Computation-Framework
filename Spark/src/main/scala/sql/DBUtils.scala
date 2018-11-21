package sql

import java.sql.{Connection, DriverManager}


object DBUtils {

  val url = "jdbc:mysql://10.100.200.18:3306/report?characterEncoding=utf-8"
  val username = "u_report"
  val password = "report123456"

  classOf[com.mysql.jdbc.Driver]

  def getConnection(): Connection = {
    DriverManager.getConnection(url, username, password)
  }

  def close(conn: Connection): Unit = {
    try{
      if(!conn.isClosed() || conn != null){
        conn.close()
      }
    }
    catch {
      case ex: Exception => {
        ex.printStackTrace()
      }
    }
  }

}
