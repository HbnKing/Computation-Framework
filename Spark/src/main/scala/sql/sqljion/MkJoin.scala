package sql.sqljion

import com.mongodb.spark.MongoSpark
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.functions.{collect_list, struct}
import org.apache.spark.sql.{SparkSession, functions}

/**
  *
  * @author wangheng
  * @create 2019-01-30 下午12:22
  * @desc
  *
  **/
object MkJoin {

  def main(args: Array[String]): Unit = {

    /*val conf = new SparkConf().setMaster("local[*]").setAppName("app")
    val sc = new SparkContext(conf)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)*/
    //val dfsAll = sqlContext.read.json("file:/Users/wh/IdeaProjects/Computation-Framework/Spark/testdata/mk1")


    //.config("spark.mongodb.output.uri", "mongodb://localhost:27017/test.output")
    //
    val spark = SparkSession.builder.master("local").appName("MongoSparkConnectorIntro").config("spark.mongodb.input.uri", "mongodb://192.168.3.131:27017/MK_Test.Tag_Field?replicaSet=wh").getOrCreate

    val jsc = new JavaSparkContext(spark.sparkContext)
    //默认 mongoSpark  返回的是 JavaMongoRdd
    // 返回为 DataSet 或者 dataFRAME  这样处理之后 保存到 mongo中 如果 _id  存在  会 upsert
    jsc.setLogLevel("warn")

    val dfsAll = MongoSpark.load(jsc).toDF
    //dfsAll.where("u = 196").show()





    /**
      * 大 U  关键字段
      * where(dfs.col("tag.tagIds") == )
      *
      */
    val dfsU =dfsAll
      //.select("_id" ,"tag.tagIds","relatedID")
      //.withColumn("tags",functions.explode(functions.col("tagIds")))
      //.withColumn("tag",functions.explode(functions.col("tags")))
      //.where("tag = 100001  or  tag =  200001").select("_id" ,"relatedID","tag")
      .select("_id" ,"relatedID")
      .withColumn("related",functions.explode(functions.col("relatedID")))
      //.where("related = 810 or related = 1559232")
      //.select("_id" ,"relatedID","related")
      //.where("related = 196 or related = 2447005")
    //dfsU.show()
    dfsU.printSchema()

    /**
      * 小 u 表 数据
      *
      */
    val dfsu =dfsAll
      //.where("u = 2447005 or u = 196")
      .select("u" ,"Profile")
      .withColumn("pro",functions.explode(functions.col("Profile")))
      .select("u","pro")

    dfsu.where("u = 196 or u = 2447005").show(false)
    println("-----")
    dfsu.printSchema()
    println("-----")



    dfsU.where("related = 196  or  related = 2447005").show(false)
    //dfsU.join(dfsu,dfsU("related") === (dfsu("u"))).show(1000,false)
    dfsU.join(dfsu,dfsU("related") === (dfsu("u")),"inner")
     .createTempView("newtable")

    //测试聚合后数据
    val checkdata =  spark.sql("select * from newtable  where  related = 196 or  related = 2447005 or  u = 196 or  u = 2447005 ")


    checkdata.show(false)
    checkdata.printSchema()
    // 聚合后获取 排序
    spark.sql("select * ,Row_Number() OVER (partition by  _id , pro.fieldID  ORDER BY pro.weight desc) ranks FROM newtable")
      //.sort("relatedID")
      .createTempView("tmpview")
    //spark.sql("select * from  tmpview").show(1000,false)


    //测试聚合后数据
    println("line 96")
    spark.sql("select * from tmpview  where  related = 196 or  related = 2447005 or  u = 196 or  u = 2447005 ").show(false)


    val  result = spark.sql("select * from tmpview WHERE ranks = 1 ")
    println("line 100")
    result.where("related = 196 or  related = 2447005 or  u = 196 or  u = 2447005 ").show(1000,false)
    //spark.sql("select * from tmpview WHERE u = 196 or u = 2447005").show(1000,false)

    println("line 104")
    result.groupBy("relatedID")
      .agg(collect_list(struct("pro.fieldID" ,"pro.sourceID","pro.value","pro.weight")).as("prof"))
      .show(1000,false)



    //spark.sql("select * from (select * ,Row_Number() OVER (partition by relatedID , pro.fieldID  ORDER BY pro.weight desc) ranks FROM newtable )  mon WHERE mon.ranks=1 ")

    /*
    .groupBy("relatedID")

   .agg(
      collect_list(struct("pro.fieldID" ,"pro.sourceID","pro.value","pro.weight")).as("prof")
    ).show(100,false)*/


    //sqlContext.sql("select * from (select * ,Row_Number() OVER (partition by subId , student ORDER BY grade desc) ranks FROM newtable )  mon WHERE mon.ranks=1")
   /*sqlContext.sql("select * from (select * ,Row_Number() OVER (partition by relatedID , Profile.fieldID  ORDER BY Profile.weight desc) ranks FROM newtable )  mon WHERE mon.ranks=1 ")
      .groupBy("relatedID").agg(
      collect_list(struct("Profile.fieldID", "Profile.value","Profile.weight","Profile.sourceID")).as("pro")
    ).show()*/

  }

}
