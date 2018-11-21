package sql



import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import org.apache.spark.sql.SparkSession


object JDBCDemo {


  def main(args: Array[String]): Unit = {

    //获取spark的连接
    val spark = SparkSession.builder().appName(JDBCDemo.getClass.getSimpleName).getOrCreate()

    val oracleurl = "jdbc:oracle:thin:@172.16.230.11:1521:zdxdb"
    val mysqlurl = "jdbc:mysql://10.100.200.18:3306/report?characterEncoding=utf-8"
    val orgTableName = "FRONTBANK.LOAN_BASE"
    val columnName = "id"
    val lowerBound = 1
    val upperBound = 1000000
    val numPartitions = 10
    val operation_record_table="operation_record"
    val org_log_table="FRONTBANK.LOAN_BASE_LOG"
    val dest_table="LOAN_BASE"
 //获取到mysql的连接
    val mysqlprop = new Properties()
    mysqlprop.setProperty("user", "u_report")
    mysqlprop.setProperty("password", "report123456")
//获取到oracle的连接
    val oracleprop = new Properties()
    oracleprop.setProperty("user", "frontbank")
    oracleprop.setProperty("password", "sdff23s")


    //获取到当前日期然后和时间进行拼接，然后转化成，long类型，最后和获取的当前的时间戳做比较
    val currentTime1 = new SimpleDateFormat("yyyy-MM-dd").format(new Date)
    val beginTimes = currentTime1 + " 09:00:00"
    val endTimes = currentTime1 + " 24:00:00"
    val bTime: Long = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(beginTimes).getTime
    val eTime: Long = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(endTimes).getTime
    //获取到当前的时间戳
    val currentTime = System.currentTimeMillis()


//读取oracle当中的指定表的数据
    val jdbcDF = spark.read.jdbc(oracleurl,orgTableName, columnName, lowerBound, upperBound,numPartitions,oracleprop)

    //  读取操作记录表里面的数据，首先判断工作流是否能执行，然后判断是第一次执行还是，导入完成后的更新操作
     val operation_record = spark.read.jdbc(mysqlurl, operation_record_table, mysqlprop)
     val is_exe:Int = operation_record.select("is_exe").rdd.map(x =>x(0).toString.toInt).collect()(0);
     val signal:Int = operation_record.select("signal").rdd.map(x =>x(0).toString.toInt).collect()(0);

    //读取源数据表当中的数据
    val first_test = spark.read.jdbc(oracleurl, orgTableName, oracleprop)
    val or= new Operation_record()
    val isexerecord1 = or.IsExeRecord(1,1) //开始执行就设置为1，最后在设置为0
    val isexerecord0 = or.IsExeRecord(1,0)
    val signalrecord1 = or.SignalRecord(1,1)
    if(is_exe==0){
      //将开始标志赋值为1，不让下个程序调用。将其赋值为1
      or.modifyisexe(isexerecord1,operation_record_table);

      //如果是0则执行整表导入，并且导入完成之后将其赋值为1
      if(signal==0){

                if (currentTime >= bTime && currentTime <= eTime) {

                  first_test.coalesce(10).write.mode(SaveMode.Append).jdbc(mysqlurl, dest_table, mysqlprop)
                  //更新状态，后续只有更新插入操作。
                  or.modifysignalrecord(signalrecord1, operation_record_table)
                  or.modifyisexe(isexerecord0, operation_record_table);
                } else {
                  println("不在设置的时间段内，数据迁移不执行")
                }
      }else{

            jdbc_coperation(oracleurl,oracleprop,mysqlurl,mysqlprop,orgTableName,operation_record_table,org_log_table,dest_table)
            or.modifyisexe(isexerecord0,operation_record_table);
      }

    }else{
      println("数据库状态为1，正在执行数据迁移")
    }

    //关闭spark连接
    spark.close()

    def jdbc_coperation(oracleurl: String,
                        oracleprop: Properties,
                        mysqlurl: String,
                        mysqlprop: Properties,
                        orgTableName: String,
                        operation_record_table: String,
                        org_log_table: String,
                        dest_table: String) : Unit ={

      //1查操作表最后一条记录的最后操作id
      //查询操作表中的记录，然后查询出里面的max_id
      val operation_record = spark.read.jdbc(mysqlurl, operation_record_table, mysqlprop)
      val max_id:Int = operation_record.select("max_id").rdd.map(x =>x(0).toString.toInt).collect()(0);

      //2查监控表大于最后操作id的记录集合
      val test_log = spark.read.jdbc(oracleurl,org_log_table, oracleprop)
      //根据操作表当中的最大ID，获取到记录表当中的数据集合,这里做限制每次操作10条记录
      val id = test_log.filter(test_log("id")>max_id)

      //初始化条数为0，计算如果超过10条按10条计算，没超过就正常的条数计算

      val countnum:Int= id.count().toInt

      //3根据第二步的记录集合查询源表的记录（需要采集的字段）
      //4更新或者插入mysql目标库的表数据

      //根据table_id 获取到源表当中的对应记录
      //读取源表中的数据。
      val test = spark.read.jdbc(oracleurl, orgTableName, oracleprop)

      for( a <- 0 until countnum){
        //查询记录表中的数据的操作状态是否是0还是1还是2
        val opertation_type:Int = id.select("opertation_type").rdd.map(x =>x(0).toString.toInt).collect()(a);
        val table_id:Int=id.select("table_id").rdd.map(x =>x(0).toString.toInt).collect()(a);
        //查询在原表中的数据
        val org_data= test.filter(test("id")===table_id)
        //拿出查询记录当中的字段

        val op = new Operators()
        if (opertation_type!=0){
          //创建连接数据操作对象
          if(opertation_type==1){
            println("插入数据到目标表当中")
            //使用dataframe直接将数据插入到mysql数据库当中，不用转换为对象
            org_data.coalesce(1).write.mode(SaveMode.Append).jdbc(mysqlurl, dest_table, mysqlprop)

          }else{
            println("将目标库中的数据删除掉，然后再将新的数据插入进去")
            op.delete(table_id,dest_table)
            org_data.coalesce(1).write.mode(SaveMode.Append).jdbc(mysqlurl, dest_table, mysqlprop)
          }
        }else{
          println("删除表中的数据")
          op.delete(table_id,dest_table)
        }
      }
      //5更新插入一条新的操作记录到记录表
      val max_id_count= max_id+countnum
      val or= new Operation_record()
      val record_id:Int=1
      val record = or.Record(record_id,max_id_count)
      //始终删除ID=1的数据,更新最大ID
      or.modify(record,operation_record_table);
    }

  }

}
