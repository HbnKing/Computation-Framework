package sql

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  *
  * @author wangheng
  * @create 2018-11-21 下午1:29
  * @desc
  *
  **/
object HiveConnector {

    def main(args: Array[String]): Unit = {
      //val url = "jdbc:oracle:thin:@172.16.250.10:1521:stupor"
      val url = "jdbc:mysql://wh:3306/test"
      //val url2 = "jdbc:mysql://10.100.200.18:3306/report?characterEncoding=utf-8"
      val url2 = "jdbc:mysql://wh:3306/test?characterEncoding=utf-8"
      val tableName = "pointer"
      val columnName = "id"
      val lowerBound = 189601025
      val upperBound = 193292990
      val numPartitions = 10

      val spark = SparkSession.builder().master("local").appName(TransferDatas.getClass.getSimpleName).getOrCreate()

      // 设置连接用户&密码
      val prop = new Properties()
      prop.setProperty("user","root")
      prop.setProperty("password","root")


      val prop2 = new Properties()
      prop2.setProperty("user", "root")
      prop2.setProperty("password", "root")

      // 取得该表数据
      val jdbcDF = spark.read.jdbc(url,tableName, columnName, lowerBound, upperBound,numPartitions,prop)

      jdbcDF.foreach(x => println(x.toString()))
      //查询部分字段
      val org_data= jdbcDF.select("id","personid","logtime","xlocation","ylocation","inserttime","tag2")
      //写会数据表 ，如果没有会自动创建 该表
      org_data.repartition(10).write.mode(SaveMode.Append).jdbc(url2, "newtable", prop2)


      spark.close()

    }





}
