package sql.sqljion

import org.apache.spark
import org.apache.spark.sql.{Column, functions}
import org.apache.spark.sql.functions.{collect_list, collect_set}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col,rank, desc,udf,struct}



/**
  *
  * @author wangheng
  * @create 2019-01-28 下午4:13
  * @desc
  *
  **/
object JionArray {



  def main(args: Array[String]): Unit = {


    /*val conf = new SparkConf().setMaster("local[*]").setAppName("app")
    val sc = new SparkContext(conf)
    val sqlContext =new SparkSession(sc).sqlContext*/

    val conf = new SparkConf().setMaster("local[*]").setAppName("app")
    val sc = new SparkContext(conf)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)


    val dfs = sqlContext.read.json("file:/Users/wh/IdeaProjects/Computation-Framework/Spark/testdata/mk2")
    val dfst = sqlContext.read.json("file:/Users/wh/IdeaProjects/Computation-Framework/Spark/testdata/mk2")


    /**
      *
      * 简单的
      * 分组后取 top1
      * 各科成绩前三名
      *
      */

    dfs.createTempView("original")
    val w = Window.partitionBy("stuId").orderBy(desc("grade"))
    //dfs.withColumn("rank", rank.over(w)).where()//.where("rank" <= 1)


    //SELECT ID,SID,SCORE FROM (
    //
    //SELECT ID,SID,SCORE, Row_Number() OVER (partition by SID ORDER BY SCORE desc) ranks FROM STUDENT
    //
    //)  mon WHERE mon.ranks=1

    //sqlContext.sql(" select * from StudentGrade t where (select count(1)  from StudentGrade where stuId=t.stuId and grade>t.grade)<1 order by subId desc ").show()

    //sqlContext.sql(" select * from (select * ,Row_Number() OVER (partition by subId ORDER BY grade desc) ranks FROM StudentGrade )  mon WHERE mon.ranks=1 ").show()
    //sqlContext.sql(" select * from (select * ,Row_Number() OVER (partition by subId ORDER BY grade desc) ranks FROM StudentGrade )  mon WHERE mon.ranks=1 ").show()
    //dfs.createTempView("StudentGrade")

    //sqlContext.sql(" select * from (select * ,Row_Number() OVER (partition by subId ORDER BY grade desc) ranks FROM StudentGrade )  mon WHERE mon.ranks=1 ").show()
    //sqlContext.sql("select stuId ,subId grade   from  original ").show()

    /*
        dfs.select("class","student").withColumn("related", functions.explode(functions.col("student")))
          .join(dfs.select("u","value","weight"),functions.col("related").===(functions.col("u")))
    .groupBy("relatedid","Profile.fieldID")
    .agg(
    )*/


    dfs.select("class","student").withColumn("relatedStuId", functions.explode(functions.col("student")))
      .join(dfst.select("stuId","subId","grade"),functions.col("relatedStuId").===(functions.col("stuId"))).createTempView("newtable")



    //sqlContext.sql("select * from (select * ,Row_Number() OVER (partition by subId , student ORDER BY grade desc) ranks FROM newtable )  mon WHERE mon.ranks=1")
    sqlContext.sql("select * from (select * ,Row_Number() OVER (partition by subId , student ORDER BY grade desc) ranks FROM newtable )  mon WHERE mon.ranks=1 ")
      .groupBy("class").agg(
      collect_list(struct("stuId", "subId","grade")).as("stu")
    ).select("stu.stuId").show()

//.agg(functions.first("u")as  "u" , functions.grouping("u") as  "u",collect_list("weight") as  "weight").show()
//.groupBy("related")
//.agg(functions.first("text") as "text", functions.collect_list("term") as "tag_terms")
//dfs.printSchema()




}

}
