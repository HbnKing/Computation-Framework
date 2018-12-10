package sparkcore

import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * @author wangheng
  * @create 2018-11-21 下午3:42
  * @desc
  *
  **/
class WordCount_scala {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("wordcount").setMaster("local")
    val sc = new SparkContext(conf)
    val  lines = sc.textFile("testdata/spark.txt")


    val words = lines.flatMap { line => line.split(" ") }
    val pairs = words.map { word => (word, 1) }
    val wordCounts = pairs.reduceByKey { _ + _ }

    wordCounts.foreach(wordCount => println(wordCount._1 + " appeared " + wordCount._2 + " times."))

  }

}
