package cdh.hbase.example;

import java.io.IOException;
import java.util.Iterator;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.KeyValueSortReducer;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class PutHFileAndBulkLoadToHBase {

/*	public static class WordCountMapper extends Mapper<LongWritable, Text, Text, Text>
	{

		private Text wordText=new Text();
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String line=value.toString();
			String[] wordArray=line.split("\t");
			
			wordText.set(wordArray[0] +wordArray[1]);
			context.write(wordText, value);
			
		}
	}*/
	
	/*public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>
	{

		private IntWritable result=new IntWritable();
		protected void reduce(Text key, Iterable<IntWritable> valueList,
				Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			int sum=0;
			for(IntWritable value:valueList)
			{
				sum+=value.get();
			}
			result.set(sum);
			context.write(key, result);
		}
		
	}*/
	
	public static class ConvertWordCountOutToHFileMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put>
	{

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String wordCountStr=value.toString();
			String[] wordCountArray=wordCountStr.split("\t");
			String word=wordCountArray[0];
			//int count=Integer.valueOf(wordCountArray[1]);
			//创建HBase中的RowKey
			byte[] rowKey=Bytes.toBytes(word);
			ImmutableBytesWritable rowKeyWritable=new ImmutableBytesWritable(rowKey);
			//byte[] family=Bytes.toBytes("info");
			//byte[] qualifier=Bytes.toBytes("id");
			//byte[] hbaseValue=Bytes.toBytes("keepersn");
			// Put 用于列簇下的多列提交，若只有一个列，则可以使用 KeyValue 格式
			// KeyValue keyValue = new KeyValue(rowKey, family, qualifier, hbaseValue);
			Put put=new Put(rowKey);
			int i = 1;
			put.add(Bytes.toBytes("caseinfo"), Bytes.toBytes("id"), Bytes.toBytes(wordCountArray[i++]));
			put.add(Bytes.toBytes("caseinfo"), Bytes.toBytes("keepersn"), Bytes.toBytes(wordCountArray[i++]));
			put.add(Bytes.toBytes("caseinfo"), Bytes.toBytes("tracktime"), Bytes.toBytes(wordCountArray[i++]));
			put.add(Bytes.toBytes("location"), Bytes.toBytes("coordx"), Bytes.toBytes(wordCountArray[i++]));
			put.add(Bytes.toBytes("location"), Bytes.toBytes("coordy"), Bytes.toBytes(wordCountArray[i++]));
			put.add(Bytes.toBytes("otherinfo"), Bytes.toBytes("workgridcode"), Bytes.toBytes(wordCountArray[i++]));
			put.add(Bytes.toBytes("otherinfo"), Bytes.toBytes("inserttime"), Bytes.toBytes(wordCountArray[i++]));
			put.add(Bytes.toBytes("otherinfo"), Bytes.toBytes("errordesc"), Bytes.toBytes(wordCountArray[i++]));
			put.add(Bytes.toBytes("otherinfo"), Bytes.toBytes("errorcode"), Bytes.toBytes(wordCountArray[i++]));
			
			context.write(rowKeyWritable, put);
		}
		
	}
	
	public static void main(String[] args) throws Exception {
		String tableName = "wordexample";
		/* Configuration conf  ;
		
		//表操作
		
			conf = HBaseConfiguration.create();
			
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
		    admin.createTable(htd, splitKeys );*/
		 
		// TODO Auto-generated method stub
        Configuration hadoopConfiguration=new Configuration();
        String[] dfsArgs = new GenericOptionsParser(hadoopConfiguration, args).getRemainingArgs();
		
        //第一个Job就是普通MR，输出到指定的目录
       /* Job job=new Job(hadoopConfiguration, "wordCountJob");
        job.setJarByClass(PutHFileAndBulkLoadToHBase.class);
        job.setMapperClass(WordCountMapper.class);
        //job.setReducerClass(WordCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.setInputPaths(job, new Path(dfsArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(dfsArgs[1]));
        //提交第一个Job
        int wordCountJobResult=job.waitForCompletion(true)?0:1;*/
        
        //如果表已经存在就先删除
       
        
        //第二个Job以第一个Job的输出做为输入，只需要编写Mapper类，在Mapper类中对一个job的输出进行分析，并转换为HBase需要的KeyValue的方式。
        Job convertWordCountJobOutputToHFileJob=new Job(hadoopConfiguration, "wordCount_bulkload");
        
        convertWordCountJobOutputToHFileJob.setJarByClass(PutHFileAndBulkLoadToHBase.class);
        convertWordCountJobOutputToHFileJob.setMapperClass(ConvertWordCountOutToHFileMapper.class);
		//ReducerClass 无需指定，框架会自行根据 MapOutputValueClass 来决定是使用 KeyValueSortReducer 还是 PutSortReducer
		//convertWordCountJobOutputToHFileJob.setReducerClass(KeyValueSortReducer.class);
        convertWordCountJobOutputToHFileJob.setMapOutputKeyClass(ImmutableBytesWritable.class);
        convertWordCountJobOutputToHFileJob.setMapOutputValueClass(Put.class);
        convertWordCountJobOutputToHFileJob.setNumReduceTasks(160);
        
        //以第一个Job的输出做为第二个Job的输入
        FileInputFormat.addInputPath(convertWordCountJobOutputToHFileJob, new Path(dfsArgs[1]));
        FileOutputFormat.setOutputPath(convertWordCountJobOutputToHFileJob, new Path(dfsArgs[2]));
        //创建HBase的配置对象
        Configuration hbaseConfiguration=HBaseConfiguration.create();
        //创建目标表对象
        HTable wordCountTable =new HTable(hbaseConfiguration, tableName);
        HFileOutputFormat.configureIncrementalLoad(convertWordCountJobOutputToHFileJob,wordCountTable);
       
        //提交第二个job
        int convertWordCountJobOutputToHFileJobResult=convertWordCountJobOutputToHFileJob.waitForCompletion(true)?0:1;
        
        //当第二个job结束之后，调用BulkLoad方式来将MR结果批量入库
        LoadIncrementalHFiles loader = new LoadIncrementalHFiles(hbaseConfiguration);
        //第一个参数为第二个Job的输出目录即保存HFile的目录，第二个参数为目标表
        loader.doBulkLoad(new Path(dfsArgs[2]), wordCountTable);
        
        //最后调用System.exit进行退出
        System.exit(convertWordCountJobOutputToHFileJobResult);
		
	}
	
	private static byte[][] getSplitKeys() {  
        String[] keys = new String[] { "11000000000000000|", "12500000000000000|", "15000000000000000|", "17500000000000000|",  
                "20000000000000000|", "21000000000000000|", "22000000000000000|", "23000000000000000|" ,
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