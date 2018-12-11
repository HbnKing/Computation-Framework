package sql.read

import org.apache.spark.sql.SparkSession

/**
  *
  * @author wangheng
  * @create 2018-12-10 下午6:24
  * @desc
  *
  **/
object HiveReader2 {
  def main(args: Array[String]): Unit = {
    val warehouseLocation = "/Users/wh/IdeaProjects/warehouse"
    //todo:1、创建sparkSession
    val hive: SparkSession = SparkSession.builder()
      .appName("HiveSupport")
      .master("local[2]")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport() //开启支持hive
      .getOrCreate()
    hive.sparkContext.setLogLevel("WARN")  //设置日志输出级别
    //import spark.implicits._
    //import spark.sql

    //todo:2、操作sql语句
    //sql("CREATE TABLE IF NOT EXISTS person (id int, name string, age int) row format delimited fields terminated by ' '")
    //sql("LOAD DATA LOCAL INPATH '/person.txt' INTO TABLE person")
    hive.sql("select * from pointer2 ").show()
    hive.stop()
  }


}
