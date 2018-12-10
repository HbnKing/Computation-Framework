package springbootspark.spark;


public class SparkConfiguration {

   /* @Bean
    public SparkConf getSparkConf() {
        SparkConf sparkConf = new SparkConf().setAppName("programdept")
                .setMaster("yarn-client")
                .set("spark.executor.uri", "hdfs://10.100.200.11:8020/spark/binary/spark-sql.tar.gz")
                .set("spark.testing.memory", "2147480000")
                .set("spark.sql.hive.verifyPartitionPath", "true")
                .set("spark.yarn.executor.memoryOverhead", "2048m")
                .set("spark.dynamicAllocation.enabled", "true")
                .set("spark.shuffle.service.enabled", "true")
                .set("spark.dynamicAllocation.executorIdleTimeout", "60")
                .set("spark.dynamicAllocation.cachedExecutorIdleTimeout", "18000")
                .set("spark.dynamicAllocation.initialExecutors", "3")
                .set("spark.dynamicAllocation.maxExecutors", "10")
                .set("spark.dynamicAllocation.minExecutors", "3")
                .set("spark.dynamicAllocation.schedulerBacklogTimeout", "10")
                .set("spark.eventLog.enabled", "true")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .set("spark.hadoop.yarn.resourcemanager.hostname", "10.100.200.11")
                .set("spark.hadoop.yarn.resourcemanager.address", "10.100.200.11:8032")
                .set("spark.hadoop.fs.defaultFS", "hdfs://10.100.200.11:8020")
                .set("spark.yarn.access.namenodes", "hdfs://10.100.200.11:8020")
                .set("spark.history.fs.logDirectory", "hdfs://10.100.200.11:8020/spark/historyserverforSpark")
                .set("spark.cores.max","10")
                .set("spark.mesos.coarse","true")
                .set("spark.executor.cores","2")
                .set("spark.executor.memory","4g")
                .set("spark.eventLog.dir", "hdfs://10.100.200.11:8020/spark/eventLog")
                .set("spark.sql.parquet.cacheMetadata","false")
                .set("spark.sql.hive.verifyPartitionPath", "true")
                //下面这个在打包之后需要解开注释，setJars是需要的，需要打一个无依赖包的jar让spark传到yarn中
//                .setJars(new String[]{"/app/springserver/addjars/sparkroute.jar","/app/springserver/addjars/fastjson-1.2.46.jar"})
                .set("spark.yarn.stagingDir", "hdfs://10.100.200.11/user/hadoop/");
//                .set("spark.sql.hive.metastore.version","2.1.0");
        String hostname = null;
        try {
            hostname = WebToolUtils.getLocalIP();
        } catch (SocketException e) {
            e.printStackTrace();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        sparkConf.set("spark.driver.host", hostname);

        return sparkConf;
    }*/
/*

    @Bean
    public SparkSession getSparkSession(@Autowired SparkConf sparkConf,@Autowired Configuration conf) {

        SparkSession.Builder config = SparkSession.builder();
        Iterator<Map.Entry<String, String>> iterator = conf.iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, String> next = iterator.next();
            config.config(next.getKey(), next.getValue());
        }

        System.out.println(config);
        SparkSession sparkSession = config.config(sparkConf).enableHiveSupport().getOrCreate();
        return sparkSession;
    }

    @Bean
    public JavaSparkContext getSparkContext(@Autowired SparkSession sparkSession){
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkSession.sparkContext());
        return javaSparkContext;
    }
*/


}
