package core.wordcount;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * @author wangheng
 * @create 2019-03-15 下午6:32
 * @desc
 *
 * 一个spark  java  demo
 *
 **/
public class WorldCount {
    public static void main(String[] args) {
        SparkConf  conf = new SparkConf().setMaster("local").setAppName("word  count");
        JavaSparkContext  sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("your path");
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }
        });


        JavaPairRDD<String, Integer> PairRdd = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });

        JavaPairRDD<String, Integer> reduceRDD = PairRdd.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        reduceRDD.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> Tuple2) throws Exception {
                System.out.println(Tuple2._1 +":" +Tuple2._2);
            }
        });
    }
}
