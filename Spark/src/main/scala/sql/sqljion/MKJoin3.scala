package sql.sqljion

import com.mongodb.spark.MongoSpark
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{SparkSession, functions}
import org.bson.Document

/**
  *
  * @author wangheng
  * @create 2019-02-14 下午2:04
  * @desc
  *
  **/
object MKJoin3 {


  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.master("local").appName("MongoSparkConnectorIntro").config("spark.mongodb.input.uri", "mongodb://192.168.3.131:27017/MK_Test.Tag_Field?replicaSet=wh").getOrCreate
    val jsc = new JavaSparkContext(spark.sparkContext)
    jsc.setLogLevel("warn")

    val dfsAll = MongoSpark.load(jsc).toDF
    /**
      * 大 U  关键字段
      * where(dfs.col("tag.tagIds") == )
      *
      */
    val dfsU = dfsAll
      .select("_id", "relatedID")

      .withColumn("related", functions.explode(functions.col("relatedID")))

    dfsU.printSchema()

    /**
      * 小 u 表 数据
      *
      */
    val dfsu = dfsAll
      .select("u", "Profile")
      .withColumn("pro", functions.explode(functions.col("Profile")))
      .select("u", "pro")




    //join  后的数据是正常的
    val result = dfsU.join(dfsu, dfsU("related") === (dfsu("u")), "inner")

    result.printSchema()
    result.show(5 ,false)



    val  groupRDD = result.map(row =>(row.getString(1),row)).groupByKey()

    //groupRDD.map()


  }

  }
