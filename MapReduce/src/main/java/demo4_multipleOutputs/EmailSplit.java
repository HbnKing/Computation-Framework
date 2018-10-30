package com.hadoop.mapreduce;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;




//我们期望统计邮箱出现次数并按照邮箱的类别，将这些邮箱分别输出到不同文件路径下。数据集示例如下所示。
/*wolys@21cn.com
zss1984@126.com
294522652@qq.com
simulateboy@163.com
zhoushigang_123@163.com*/

public class EmailSplit extends Configured implements Tool {
	public static class EmailMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
		private final static IntWritable one = new IntWritable(1); 
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			context.write(value, one);
		}
	}
	public static class EmailReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();
		private MultipleOutputs< Text, IntWritable> multipleOutputs;
	protected void setup(Context context) {
		multipleOutputs = new MultipleOutputs<Text, IntWritable>(context);
	}
	protected void reduce(Text key,Iterable< IntWritable> values,Context context) throws IOException, InterruptedException {
		int begin =key.toString().indexOf("@");
		int end =key.toString().indexOf(".");
		if(begin >= end){
			return;
		}
		//根据begin 和 end ,进行截取获取邮箱类别  比如qq
		String name = key.toString().substring(begin+1, end);
		int sum = 0;
		for (IntWritable value:values) {
			sum +=value.get();
		}
		result.set(sum);
		multipleOutputs.write(key, result, name);
	}
	protected void cleanup(Context context) throws IOException ,InterruptedException{
		multipleOutputs.close();
	}
	}
	public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		Configuration conf = new Configuration();//读取配置文件
		Path myPath = new Path(args[1]);
		FileSystem hdfs =myPath.getFileSystem(conf);//设置fs 创建输出路径
		if(hdfs.isDirectory(myPath)){
			hdfs.delete(myPath, true);
		}
		
		Job job = new Job(conf);
		job.setJarByClass(EmailSplit.class);//设置主类
		//设置mapper 和reducer 的类
		job.setMapperClass(EmailMapper.class);
		job.setReducerClass(EmailReduce.class);
		//设置输入输出路径
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		//设置输出key value 类
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		 return job.waitForCompletion(true) ? 0 : 1;//提交作业
		
		
	}
	public static void main(String[] args) throws Exception{
		//设置文件输入输出路径
		String[] args0 = {"hdfs://wang:9000/mergehdfs/email/mail.txt","hdfs://wang:9000/mergehdfs/email/mailcount"};
		int ec = ToolRunner.run(new Configuration(), new EmailSplit(), args0);
		System.exit(ec);
	}

}
