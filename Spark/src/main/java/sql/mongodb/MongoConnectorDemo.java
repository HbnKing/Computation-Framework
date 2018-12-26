package sql.mongodb;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;

import java.util.HashMap;
import java.util.Map;

/**
 * @author wangheng
 * @create 2018-12-17 上午11:50
 * @desc
 *
 * 使用 spark - mongoConnecotor  来连接 mongo
 * 并查看 数据量 的demo
 **/
public class MongoConnectorDemo {
    public static void main(String[] args) throws InterruptedException {
        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("MongoSparkConnectorIntro")
                .config("spark.mongodb.input.uri", "mongodb://192.168.3.130:27017/test.pointer?replicaSet=wh")
                .config("spark.mongodb.output.uri", "mongodb://192.168.3.130:27017/test.pointer2?replicaSet=wh")
                .getOrCreate();

        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
        //默认 mongoSpark  返回的是 JavaMongoRdd
        // 返回为 DataSet 或者 dataFRAME  这样处理之后 保存到 mongo中 如果 _id  存在  会 upsert
        Dataset<Row> customRdd = MongoSpark.load(jsc).toDF();

        //写入到mongoDB
        MongoSpark.save(customRdd);

        Thread.sleep(50000);

        jsc.stop();







    }


}

