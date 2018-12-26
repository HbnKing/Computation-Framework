package sql.mongodb ;

/**
 * @author wangheng
 * @create 2018-09-03 下午12:31
 * @desc
 **/


import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import java.util.HashMap;
import java.util.Map;

import com.mongodb.BasicDBObject;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;

import static java.util.Collections.singletonList;


public final class ReadFromMongoDBReadConfig {


    public static void main(final String[] args) throws InterruptedException, ParseException, AnalysisException {

        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("MongoSparkConnectorIntro")
                .config("spark.mongodb.input.uri", "mongodb://192.168.3.130:27017/test.pointer?replicaSet=wh")
                .config("spark.mongodb.output.uri", "mongodb://192.168.3.130:27017/test.pointer2?replicaSet=wh")
                .getOrCreate();

        // Create a JavaSparkContext using the SparkSession's SparkContext object
        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

        /*Start Example: Read data from MongoDB************************/

        // Create a custom ReadConfig
        Map<String, String> readOverrides = new HashMap<String, String>();
        //readOverrides.put("collection", "spark");
       readOverrides.put("readPreference.name", "primary");


       DateFormat dateFormat  = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
       Date starter =  dateFormat.parse("2018-01-04 7:00:50");
       Date ender =  dateFormat.parse("2018-01-04 8:00:50");
       //spark.catalog().listTables("test").show();

        //System.out.println("*********");
       //spark.sql("select * from pointer" ).show();

        /*Dataset<Row> load = spark.read().format("com.mongodb.spark.sql").load().filter(new FilterFunction<Row>() {
            @Override
            public boolean call(Row row) throws Exception {
                return ((Date )row.getTimestamp(2)).compareTo(starter) >0 &&((Date) row.getTimestamp(2) ).compareTo(ender) <0  ;
            }
        }).select("insettime","xloaction","ylocation");

        load.show();*/





        ReadConfig readConfig = ReadConfig.create(jsc).withOptions(readOverrides);

        // Load data using the custom ReadConfig
        long  startertime = System.currentTimeMillis();
        JavaMongoRDD<Document> customRdd = MongoSpark.load(jsc, readConfig);

        //System.out.println(customRdd.count());
        System.out.println("第一次查询的时间是 "+ (System.currentTimeMillis()-startertime));

        customRdd.foreach(new VoidFunction<Document>() {
            @Override
            public void call(Document document) throws Exception {
                System.out.println(document.toJson());
            }
        });

        /*
        startertime = System.currentTimeMillis();
        BasicDBObject dateRange = new BasicDBObject("$lt", new Date(2018,0,4,7,0,10));
        dateRange.put("$gte", new Date(2018,0,4,7,0,0));

        BasicDBObject query = new BasicDBObject("$insettime", dateRange);

         customRdd.withPipeline(singletonList(query));

        System.out.println(System.currentTimeMillis() -startertime);
        */



        //customRdd.filter();

        System.out.println(customRdd.count());
        Thread.sleep(100000);

        /*End Example**************************************************/


        // Analyze data from MongoDB
        System.out.println(customRdd.count());
        customRdd.foreach(new VoidFunction<Document>() {
            @Override
            public void call(Document document) throws Exception {

                System.out.println(document.toJson());
            }
        });
        System.out.println(customRdd.first().toJson());
        Thread.sleep(Long.MAX_VALUE);

        jsc.close();

    }

}