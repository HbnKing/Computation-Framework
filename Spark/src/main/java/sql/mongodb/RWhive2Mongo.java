package sql.mongodb;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Map;

/**
 * @author wangheng
 * @create 2018-12-11 下午2:05
 * @desc
 *
 * 从 hive 读取数据并且写入MongoDB
 **/
public class RWhive2Mongo {

    public static void main(String[] args) {
        SparkSession  sparkSession  = SparkSession.builder().master("local").appName("hive2mongo").enableHiveSupport().getOrCreate();

        Dataset<Row> hivesql = sparkSession.sql("select * from  pointer2");


        String uriStr = "mongodb://192.168.3.130:27017/test.pointer?replicaSet=wh";

        Map link = new HashMap();
        link.put("spark.mongodb.output.uri",uriStr);

        hivesql.write().options(link).mode(SaveMode.Overwrite).format("com.mongodb.spark.sql").save();


    }
}
