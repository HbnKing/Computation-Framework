package sql

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * 操作数据库的相关方法
 */
class Operators {

  case class User(id: Int, name: String)


  //根據id刪除目標表當中的数据
  def delete(id: Int,dest_table:String): Boolean ={
    val conn = DBUtils.getConnection()
    try{
      val sql = "DELETE FROM "+dest_table+ "WHERE id = ?"
      val pstm = conn.prepareStatement(sql)
      pstm.setObject(1, id)

      pstm.executeUpdate() > 0
    }
    finally {
      DBUtils.close(conn)
    }
  }


}
