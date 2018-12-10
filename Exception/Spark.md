1 spark 连接  hive  的异常   
`Exception in thread "main" java.lang.IllegalArgumentException: Unable to instantiate SparkSession with Hive support because Hive classes are not found.
  	at org.apache.spark.sql.SparkSession$Builder.enableHiveSupport(SparkSession.scala:865)
  	at sql.Write2Hive$.main(Write2Hive.scala:15)
  	at sql.Write2Hive.main(Write2Hive.scala)`
  	
跟踪SparkSession.scala源码发现如下代码：

`/** * Return true if Hive classes can be loaded, otherwise false. */ private[spark] def hiveClassesArePresent: Boolean = { try { Utils.classForName(HIVE_SESSION_STATE_CLASS_NAME) Utils.classForName(HIVE_SHARED_STATE_CLASS_NAME) Utils.classForName("org.apache.hadoop.hive.conf.HiveConf") true } catch { case _: ClassNotFoundException | _: NoClassDefFoundError => false } }`
原来是缺少jar包，，查询所需要的三个类所需要的maven引用并添加到pom中，解决导致classForName失败
[SOF](https://stackoverflow.com/questions/39444493/how-to-create-sparksession-with-hive-support-fails-with-hive-classes-are-not-f)