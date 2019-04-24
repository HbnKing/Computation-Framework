package org.mapreduce.customize;

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

public class Email extends Configured implements Tool {

	public static class MailMapper extends Mapper< LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			context.write(value, one);
		}
	}

	public static class MailReducer extends Reducer< Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();
		private MultipleOutputs< Text, IntWritable> multipleOutputs;

		@Override
		protected void setup(Context context) throws IOException ,InterruptedException{
			multipleOutputs = new MultipleOutputs< Text, IntWritable>(context);
		}
		protected void reduce(Text Key, Iterable< IntWritable> Values,Context context) throws IOException, InterruptedException {
			int begin = Key.toString().indexOf("@");
            int end = Key.toString().indexOf(".");
            if(begin>=end){
            	return;
            }
            //获取邮箱类别，比如 qq
            String name = Key.toString().substring(begin+1, end);
			int sum = 0;
			for (IntWritable value : Values) {
				sum += value.get();
			}
			result.set(sum);
			multipleOutputs.write(Key, result, name);
		}
		@Override
		protected void cleanup(Context context) throws IOException ,InterruptedException{
			multipleOutputs.close();
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();// 读取配置文件
		
		Path mypath = new Path(args[1]);
		FileSystem hdfs = mypath.getFileSystem(conf);//创建输出路径
		if (hdfs.isDirectory(mypath)) {
			hdfs.delete(mypath, true);
		}
		Job job = Job.getInstance();// 新建一个任务
		job.setJarByClass(Email.class);// 主类
		
		FileInputFormat.addInputPath(job, new Path(args[0]));// 输入路径
		FileOutputFormat.setOutputPath(job, new Path(args[1]));// 输出路径

		job.setMapperClass(MailMapper.class);// Mapper
		job.setReducerClass(MailReducer.class);// Reducer
		
		job.setOutputKeyClass(Text.class);// key输出类型
		job.setOutputValueClass(IntWritable.class);// value输出类型
		
		job.waitForCompletion(true);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		String[] args0 = {
				"hdfs://single.hadoop.dajiangtai.com:9000/junior/mail.txt",
				"hdfs://single.hadoop.dajiangtai.com:9000/junior/mail-out/" };
		int ec = ToolRunner.run(new Configuration(), new Email(), args0);
		System.exit(ec);
	}
}