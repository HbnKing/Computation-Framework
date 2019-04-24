package org.mapreduce.examples;

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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class AmeicanTemperture   extends Configured implements Tool{

	/**
	 * @function main 方法
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		//数据输入路径和输出路径
		String[] args0 = {
							"hdfs://192.168.0.30:8020/user/wh/weather/",
							"hdfs://192.168.0.30:8020/user/wh/weather/out/"
						};
		int ec = ToolRunner.run(new Configuration(), new AmeicanTemperture(), args0);
		System.exit(ec);
	}
	
	/**
	 * @function 任务驱动方法
	 * @param args
	 * @return
	 * @throws Exception
	 */
	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();//读取配置文件

		Path mypath = new Path(args[1]);
		FileSystem hdfs = mypath.getFileSystem(conf);
		if (hdfs.isDirectory(mypath)) {
			hdfs.delete(mypath, true);
		}

		Job job = new Job(conf, "temperature");//新建一个任务
		job.setJarByClass(AmeicanTemperture.class);// 设置主类
		
		FileInputFormat.addInputPath(job, new Path(args[0]));// 输入路径
		FileOutputFormat.setOutputPath(job, new Path(args[1]));// 输出路径

		job.setMapperClass(TemperatureMapper.class);// Mapper
		job.setReducerClass(TemperatureReducer.class);// Reducer
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);		
		return job.waitForCompletion(true)?0:1;//提交任务
	}

	
	
	
	public static class TemperatureMapper extends Mapper< LongWritable, Text, Text, IntWritable> {
		/**
		 * @function Mapper 解析气象站数据
		 * @input key=偏移量  value=气象站数据
		 * @output key=weatherStationId value=temperature
		 */
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String line = value.toString(); //每行气象数据
			int temperature = Integer.parseInt(line.substring(14, 19).trim());//每小时气温值
			if (temperature != -9999) { //过滤无效数据				
				FileSplit fileSplit = (FileSplit) context.getInputSplit();
				String weatherStationId = fileSplit.getPath().getName().substring(5, 10);//通过文件名称提取气象站id
				context.write(new Text(weatherStationId), new IntWritable(temperature));
			}
		}
	}
	
	
	public static class TemperatureReducer extends Reducer< Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable< IntWritable> values,Context context) throws IOException, InterruptedException {
			int sum = 0;
			int count = 0;
			//统计每个气象站的气温值总和
			for (IntWritable val : values) {
				sum += val.get();
				count++;
			}
			//求每个气象站的气温平均值
			result.set(sum / count);
			context.write(key, result);
		}
	}

}
