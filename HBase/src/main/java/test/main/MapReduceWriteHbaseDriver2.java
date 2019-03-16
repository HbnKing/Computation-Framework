package test.main;


import java.io.IOException;
import java.util.Iterator;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
/**
 * 
 *  将hdfs中的数据导入hbase
 *
 */
public class MapReduceWriteHbaseDriver2 {

	//hbase 的数据全部为String 类型   ImmutableBytesWritable IntWritable
	public static class WordCountMapperHbase extends
			Mapper<Object, Text, ImmutableBytesWritable, Text> {
		private final static IntWritable one = new IntWritable(1);

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String [] readlines = value.toString().split("\t");
			
			if( "30000000000000000".compareTo(readlines[0]) >0  && "20000000000000000".compareTo(readlines[0]) <0){
			//context.write(new ImmutableBytesWritable(Bytes.toBytes(readlines[0])), value);
			}
			
		}
	}
	
	public static class WordCountReducerHbase extends TableReducer<ImmutableBytesWritable, Text, ImmutableBytesWritable> {  
		private IntWritable result = new IntWritable();
		Put put = null ;

        public void reduce(ImmutableBytesWritable key, Iterable<Text> values, Context context)  
                throws IOException, InterruptedException { 

			for (Text val : values) {
		
				
				String [] readlines = val.toString().split("\t");
			if(readlines.length == 10){
			put = new Put(key.get());//put 实例化  key代表主键，每个单词存一行
			//三个参数分别为  列簇为content，列修饰符为count，列值为词频
			int i = 1;
			
			put.add(Bytes.toBytes("caseinfo"), Bytes.toBytes("id"), Bytes.toBytes(readlines[i++]));
			put.add(Bytes.toBytes("caseinfo"), Bytes.toBytes("keepersn"), Bytes.toBytes(readlines[i++]));
			put.add(Bytes.toBytes("caseinfo"), Bytes.toBytes("tracktime"), Bytes.toBytes(readlines[i++]));
			put.add(Bytes.toBytes("location"), Bytes.toBytes("coordx"), Bytes.toBytes(readlines[i++]));
			put.add(Bytes.toBytes("location"), Bytes.toBytes("coordy"), Bytes.toBytes(readlines[i++]));
			context.write(key , put);
			
			
			}
			break;
			}
        }  
    }
	
	public static void main(String[] args)throws Exception {  
		String tableName = "wordexample";//hbase 数据库表名
        Configuration conf=HBaseConfiguration.create(); //实例化Configuration 
        //conf.set("hbase.zookeeper.quorum", "hadoop1,hadoop2,hadoop3");
		//conf.set("hbase.zookeeper.property.clientPort", "2181");
		//conf.set("hbase.master", "hadoop2:60000");
   	    
   	    //如果表已经存在就先删除
       HBaseAdmin admin = new HBaseAdmin(conf);
   	    if(admin.tableExists(tableName)){
   	    	admin.disableTable(tableName);
   	    	admin.deleteTable(tableName);
   	    }
   	    
   	    HTableDescriptor htd = new HTableDescriptor(tableName);
   	    HColumnDescriptor hcd = new HColumnDescriptor("caseinfo");
   	    HColumnDescriptor hcd2 = new HColumnDescriptor("location");
   	    HColumnDescriptor hcd3 = new HColumnDescriptor("otherinfo");
   	    
   	    
   	    htd.addFamily(hcd);//创建列簇
   	    htd.addFamily(hcd2);
   	    htd.addFamily(hcd3);
   	    
   	    //admin.createTable(htd);//创建表
   	    byte[][] splitKeys = getSplitKeys();
		admin.createTable(htd, splitKeys );
   	    
        Job job=new Job(conf,"import from hdfs to hbase");  
        job.setJarByClass(MapReduceWriteHbaseDriver2.class);  
        
        job.setMapperClass(WordCountMapperHbase.class);  

        //设置插入hbase时的相关操作
        TableMapReduceUtil.initTableReducerJob(tableName, WordCountReducerHbase.class, job, null, null, null, null, false);

        job.setMapOutputKeyClass(ImmutableBytesWritable.class);  
        job.setMapOutputValueClass(Text.class);  
        
        job.setOutputKeyClass(ImmutableBytesWritable.class);  
        job.setOutputValueClass(Put.class);
        
        
        FileInputFormat.addInputPaths(job, "hdfs://hadoop1:8020/user/hive/warehouse/hbase2hive_tmp/");  
        System.exit(job.waitForCompletion(true) ? 0 : 1);  
  
    }
	
	private static byte[][] getSplitKeys() {  
        String[] keys = new String[] { "11000000000000000|", "12000000000000000|", "13000000000000000|",  "14000000000000000|","15000000000000000|", "16000000000000000|" ,"17000000000000000|", "18000000000000000|",  
        		 "19000000000000000|", "20000000000000000|", "21000000000000000|", "22000000000000000|", "23000000000000000|" ,
        "25000000000000000|", "40000000000000000|", "50000000000000000|", "60000000000000000|" ,
        "70000000000000000|", "80000000000000000|", "90000000000000000|" };  
        byte[][] splitKeys = new byte[keys.length][];  
        TreeSet<byte[]> rows = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);//升序排序  
        for (int i = 0; i < keys.length; i++) {  
            rows.add(Bytes.toBytes(keys[i]));  
        }  
        Iterator<byte[]> rowKeyIter = rows.iterator();  
        int i=0;  
        while (rowKeyIter.hasNext()) {  
            byte[] tempRow = rowKeyIter.next();  
            rowKeyIter.remove();  
            splitKeys[i] = tempRow;  
            i++;  
        }  
        return splitKeys;  
	}
}
