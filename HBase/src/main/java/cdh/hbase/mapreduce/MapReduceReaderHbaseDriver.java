package cdh.hbase.mapreduce;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/*
 * 读取hbase中的数据
 */
public class MapReduceReaderHbaseDriver {
	public static class WordCountHBaseMapper extends TableMapper<Text, Text> {

		@Override
		protected void map(ImmutableBytesWritable key, Result values,
				Context context) throws IOException, InterruptedException {
			StringBuffer sb = new StringBuffer("");
			//获取列族content下面所有的值
			for (java.util.Map.Entry<byte[], byte[]> value : values
					.getFamilyMap("content".getBytes()).entrySet()) {
				String str = new String(value.getValue());
				if (str != null) {
					sb.append(str);
				}
				context.write(new Text(key.get()), new Text(new String(sb)));
			}
		}

	}

	public static class WordCountHBaseReducer extends
			Reducer<Text, Text, Text, Text> {
		private Text result = new Text();

		public void reduce(Text key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			for (Text val : values) {
				result.set(val);
				context.write(key, result);
			}
			
		}
	}
	public static void main(String[] args)throws Exception {  
		String tableName = "wordcount";//hbase表名称
        Configuration conf=HBaseConfiguration.create(); //实例化 Configuration 
        conf.set("hbase.zookeeper.quorum", "master,slave1,slave2");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		conf.set("hbase.master", "master:60000");
   	    
   	    
        Job job=new Job(conf,"import from hbase to hdfs");  
        job.setJarByClass(MapReduceReaderHbaseDriver.class);  
        
        job.setReducerClass(WordCountHBaseReducer.class); 
        //设置读取hbase时的相关操作
        TableMapReduceUtil.initTableMapperJob(tableName, new Scan(), WordCountHBaseMapper.class,  Text.class, Text.class, job, false);
        
        FileOutputFormat.setOutputPath(job, new Path("hdfs://master:8030/dajiangtai/hbase1"));  
        System.exit(job.waitForCompletion(true) ? 0 : 1);  
  
    }
}
