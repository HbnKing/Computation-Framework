package sql.hive;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;

public class DeptController {

  /*  @Autowired
    SparkSession ss;*/

 /*   SparkSession hiveSpark = SparkSession
            .builder().enableHiveSupport().getOrCreate();
*/
    SparkConf conf = new SparkConf().setAppName("programdept").setMaster("local");
         //.set("spark.hadoop.fs.defaultFS", "hdfs://vm200-11:8020")
         //.set("spark.yarn.access.namenodes", "hdfs://vm200-11:8020")
         //.set("spark.hadoop.yarn.resourcemanager.hostname", "vm200-11")
         //.set("spark.hadoop.yarn.resourcemanager.address", "vm200-11:8032");

    JavaSparkContext sc = new JavaSparkContext(conf);

    HiveContext hiveContext = new HiveContext(sc.sc());



    public void test(){


       // hiveContext.sql("use oracle_table");
      // Dataset<Row> sql = hiveContext.sql("show tables");
      //  DataFrame sql=hiveContext.sql("show databases");
        Dataset<Row> sql = hiveContext.sql("show tables");

        //  JSONArray jsonArray = JSONArray.fromObject(json.getString("data"));
       //List<Object> collect =   sql.collectAsList().stream().map(x->x.toString()).collect(Collectors.toList());

       //  return   sql.toJSON();
    //   List<Object> collect = sql.collectAsList().stream().map(x -> x.getString(1)).collect(Collectors.toList());
      sql.show();

      //  return "123456";

    }


    public static void main(String[] args) {
        DeptController  cc = new DeptController();
        cc.test();
    }

}
