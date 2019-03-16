package core.wordcount

import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * @author wangheng
  * @create 2019-03-15 下午6:54
  * @desc
  *
  * scala  版本的wordcount
  **/
object wordcount {

  def main(args: Array[String]): Unit = {

    val  conf = new SparkConf().setAppName("scala  word count").setMaster("local")

    val  sc  = new SparkContext(conf)

    val  lines = sc.textFile("",10)

    lines.flatMap(x =>x.split(" ")).map((_,1)).reduceByKey(_+_).foreach(x=>println(x._1+":"+x._2))

  }
}
