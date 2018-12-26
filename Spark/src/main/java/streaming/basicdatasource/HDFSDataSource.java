package streaming.basicdatasource;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * @author wangheng
 * @create 2018-12-07 下午5:02
 * @desc
 *
 * 基于hdfs 文件的实时 wordcount
 * 以hdfs  上的文件作为spark streaming 的数据输入
 * 注意 只能检测到每批次 时间 内 新增的 文件
 * 文件 应当具有相同的格式 ，文件应当以重新命名 或者移动的方式 写入
 **/
public class HDFSDataSource {


    public static void main(String[] args) throws InterruptedException {
        SparkConf  conf = new SparkConf()
                .setAppName(HDFSDataSource.class.getName())
                .setMaster("local[2]");

        JavaStreamingContext  jssc = new JavaStreamingContext(conf, Durations.seconds(5));

        JavaDStream<String> stringJavaDStream = jssc.textFileStream("hdfs://wh:9000/tmp/data");



        JavaDStream<String> words = stringJavaDStream.flatMap(new FlatMapFunction<String, String>() {

            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split("\t")).iterator();
            }
        });

        JavaPairDStream<String, Integer> JavaPairDStream = words.mapToPair(new PairFunction<String, String, Integer>() {

            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        });


        JavaPairDStream.updateStateByKey(new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {
            @Override
            public Optional<Integer> call(List<Integer> values, Optional<Integer> state) throws Exception {
            //第一个参数就是key传进来的数据，第二个参数是曾经已有的数据

                Integer updatedValue = 0 ;//如果第一次，state没有，updatedValue为0，如果有，就获取
                if(state.isPresent()){
                    updatedValue = state.get();
                }
                //遍历batch传进来的数据可以一直加，随着时间的流式会不断去累加相同key的value的结果。
                for(Integer value: values){
                    updatedValue += value;
                }
                return Optional.of(updatedValue);//返回更新的值

            }
        });


        JavaPairDStream<String, Integer> totalsum = JavaPairDStream.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });


        totalsum.print();


        jssc.start();
        jssc.awaitTermination();
        jssc.close();





    }

}
