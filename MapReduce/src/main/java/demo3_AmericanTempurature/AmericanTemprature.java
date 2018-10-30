package com.work.hadoopwork;

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




/**
 * 任务要求 根据各个气象站的数据统计出30年该区域的平均气温
 *
 */
public class AmericanTemprature extends Configured implements Tool
{
/**************mapper 类的四个值分别为输入key  输入value  输出key  输出value 
 * 格式如下 (使用默认的文本格式)
 * 1985 07 31 05   133    78 -9999   250     0 -9999     0 -9999
 * 1985 07 31 06   122    72 -9999    90     0 -9999     0     0
 * 这里的输入key 为字节偏移量 即为整数型,输入value 为每行的内容
 * 输入靠后面的TextInputFormat 类控制
 * 
 * 输出outputkey 为输出的气象站的编号  输出值为  气象气温  (可设置其他)
	*********************/
	public  static  class TemperatureMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
		public void map(LongWritable key,Text value,Context context)throws IOException,InterruptedException{
			//对气象数据  读取内容转换为string
			String line = value.toString();
			
			//从value中截取气温值 
			int temperature = Integer.parseInt(line.substring(14, 19).trim());
			//从文件名称中获取气象站的编号
			
			if (temperature != -9999) { //过滤无效数据	
				//测试用写死气象站编号
				
				FileSplit fileSplit = (FileSplit) context.getInputSplit();
				
				//String weatherStationId = fileSplit.getPath().getName().substring(5, 10);//通过文件名称提取气象站id
				String weatherStationId = "03103";
				context.write(new Text(weatherStationId), new IntWritable(temperature));				
			}
		
		}
	}	
		
		/****************
		 * ************
		 * @author Administrator
		 *@reduce类
		 */
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
		
		
@SuppressWarnings("deprecation")
public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
	
	    //读取配置文件
		Configuration conf = new Configuration();//读取配置文件
		//设置输出路径  判断是否存在
		Path mypath = new Path(args[1]);
		FileSystem hdfs = mypath.getFileSystem(conf);
		if (hdfs.isDirectory(mypath)) {
			hdfs.delete(mypath, true);
		}

		//设置job  对象
		Job job = new Job(conf, "temperature");//新建一个任务
		job.setJarByClass(AmericanTemprature.class);// 设置主类
		
		FileInputFormat.addInputPath(job, new Path(args[0]));// 输入路径
		FileOutputFormat.setOutputPath(job, new Path(args[1]));// 输出路径

		job.setMapperClass(TemperatureMapper.class);// Mapper
		job.setReducerClass(TemperatureReducer.class);// reducer
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);		
		return job.waitForCompletion(true)?0:1;//提交任务
	}

	
    public static void main( String[] args0 ) throws Exception
    {
        //入口处设置输入输出路径
    	/*String[] args0 = {"hdfs://sla1:9000/weather/",
		"hdfs://sla1:9000/weather/out"};*/
    	//可以在 run configurations   arguments  传入
    	 
    	int ec =ToolRunner.run(new Configuration(),new AmericanTemprature(),args0 );
    	
    	System.exit(ec);
    }
}
