package com.hadoop.test;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;

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



/*
 * 1编写map()函数
 * 2编写reduce()函数
 * 3编写run()执行方法,负责运行mapreduce作业
 * 4在main方法中运行程序
 */

public class Temperature extends Configured implements Tool{

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		//设置输入输出路径
		String[] args0 = {"hdfs://wang:9000/weather/",
				"hdfs://wang:9000/weather/out"};
		//执行run方法[读取配置文件,主类,输入和输出路径]
		int ec =ToolRunner.run(new Configuration(),new Temperature(),args0 );
		//第三部根据返回状态退出
		System.exit(ec);
	}
	public static class TemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
		public void map(LongWritable key,Text value,Context context)throws IOException,InterruptedException{
			//第一步,将气象站数据转为string类型
			String line = value.toString();
			//第二部,获取气温值
			int temperature = Integer.parseInt(line.substring(14, 19).trim());
			//第五步过滤无效数据 判断气温值是否为有效气温值,
			if(temperature!=-9999){
			//第三部,获取气象编号
			//获取输入分片
			FileSplit filesplit = (FileSplit) context.getInputSplit();
			//然后获取气象站编号 获取路径获取名称,截取
			String weatherStationId = filesplit.getPath().getName().substring(5, 10);
			//第四步,输出数据.写入站点名称及温度值
			context.write(new Text(weatherStationId), new IntWritable(temperature));
			}

		}
	
	}

	public static class TemperatureReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		private IntWritable result = new IntWritable();
	
		public void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException{
			//第一步:统计相同气象站所有气温值
			int sum = 0;
			int count =0;
			//循环统一气象站的所有气象值
			for(IntWritable val:values){
				//对所有气温累加
				sum +=val.get();
				//统计集合大小
				count++;				
			}
			//第二部求同一个气象站的平均气温
			result.set(sum / count);
			context.write(key, result);
		}
	}

	
	
	public int run(String[] args)throws Exception{
		//第一步读取配置文件
		Configuration conf =new Configuration();
		//第二部输出路径存在就删除
		Path mypath = new Path(args[1]);
		FileSystem hdfs =mypath.getFileSystem(conf);
		if(hdfs.isDirectory(mypath)){
			hdfs.delete(mypath,true);
		}
		//第三部构建job对象
		Job job = new Job(conf,"temperature");
		job.setJarByClass(Temperature.class);
		//第四步指定数据输入路径和输出路径
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		//第五步指定mapper和reducer
		job.setMapperClass(TemperatureMapper.class);
		job.setReducerClass(TemperatureReducer.class);
		//设置map()he reduce()函数的输出类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		//第七步:提交作业
		return job.waitForCompletion(true)?0:1;
	}



}
