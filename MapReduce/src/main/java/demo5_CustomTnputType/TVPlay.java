package com.hadoop.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.hadoop.mapreduce.TVPlayCountCopy.TVPlayMapper;
import com.hadoop.mapreduce.TVPlayCountCopy.TVPlayReducer;



public class TVPlay extends Configured implements Tool{
public static void main(String[] args) throws Exception {
	String[] paths = {"hdfs://wang:9000/mapreduce/tvplay.txt","hdfs://wang:9000/mapreduce/tvout"};
	int ec = ToolRunner.run(new Configuration(), new TVPlay(), paths);
	System.exit(ec);
}
//Mapper reduce   job 主类 
//Mapper reducer 定义的输入输出类型应该与重写的inputformat 类定义的相同

		
		//mapper
public  static class TVmapper extends Mapper<Text, TVplaydata, Text, TVplaydata> {
	public void map(Text key,TVplaydata value, Context context) throws IOException, InterruptedException {
				context.write(key, value);
			}
		}
public static class TVreducer extends Reducer<Text, TVplaydata, Text, Text>{	
			private Text m_key = new Text();
			private Text m_value = new Text();
			private MultipleOutputs<Text, Text> mos;
			protected void setup(Context context) throws IOException,
					InterruptedException {
				mos = new MultipleOutputs<Text, Text>(context);
			}
		protected void reduce(Text key,Iterable<TVplaydata> values, Context context) throws IOException, InterruptedException{
				
				int tvplaynum = 0;
				int tvfavorite = 0;
				int tvcomment = 0;
				int tvvote = 0;
				int tvdown = 0;
			for (TVplaydata tv:values) {
				tvplaynum += tv.getTvpalynum();
				tvfavorite += tv.getTvfavorite();
				tvcomment += tv.getTvcomment();
				tvvote += tv.getTvvote();
				tvdown += tv.getTvdown();
				}
			//读取电视剧名称以及播放网站    
			//通过制表符分割  成为数组
			String[] records = key.toString().split("\t");
			// 1优酷2搜狐3土豆4爱奇艺5迅雷看看
			String source  = records[1];//获取媒体名称
			m_key.set(records[0]);
			m_value.set(tvplaynum+"\t"+tvfavorite+"\t"+tvcomment+"\t"+tvdown+"\t"+tvvote);
			//对类别进行判断,,如果是一个类别就输出到一个文件中
			//mos.write(source, m_key, m_value);
			if(source.equals("1")){
				mos.write("youku", m_key, m_value);
				
			}else if (source.equals("2")) {
				mos.write("souhu", m_key, m_value);
			}else if (source.equals("3")) {
				mos.write("tudou",m_key, m_value);
			}else if (source.equals("4")) {
				mos.write("aiqiyi", m_key, m_value);
			}else if (source.equals("5")) {
				mos.write("xunlei", m_key, m_value);
			}else{
				mos.write("other", m_key, m_value);
			}
		}
		protected void cleanup(Context context) throws IOException ,InterruptedException{
				mos.close();
		}	
		}
	
		@Override
public int run(String[] arg0) throws Exception {
			// TODO Auto-generated method stub
			/*Configuration conf = new Configuration();// 配置文件对象
			
			Path path = new Path(arg0[1]);
			FileSystem hdfs = path.getFileSystem(conf);// 创建输出路径
			//判断输出路径是否存在,如果存在就删除
			if(hdfs.isDirectory(path)){
				hdfs.delete(path, true);
			}
			//设置job的一些属性
			//新建job 对象  并设置主类
			Job job = new Job(conf,"tvplay");
			job.setJarByClass(TVPlay.class);
			//设置输入格式 的类
			job.setInputFormatClass(PlayinputFormat.class);
			//设置mapper
			job.setMapperClass(TVmapper.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(TVplaydata.class);
			//设置reduce
			job.setReducerClass(TVreducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			
			//设置输入输出路径
			FileInputFormat.addInputPath(job, new Path(arg0[0]));
			FileOutputFormat.setOutputPath(job,new Path(arg0[1]));
			// 自定义文件输出格式，通过路径名（pathname）来指定输出路径
			MultipleOutputs.addNamedOutput(job, "youku", TextOutputFormat.class,
					Text.class, Text.class);
			MultipleOutputs.addNamedOutput(job, "souhu", TextOutputFormat.class,
					Text.class, Text.class);
			MultipleOutputs.addNamedOutput(job, "tudou", TextOutputFormat.class,
					Text.class, Text.class);
			MultipleOutputs.addNamedOutput(job, "aiqiyi", TextOutputFormat.class,
					Text.class, Text.class);
			MultipleOutputs.addNamedOutput(job, "xunlei", TextOutputFormat.class,
					Text.class, Text.class);
			MultipleOutputs.addNamedOutput(job, "other", TextOutputFormat.class,
					Text.class, Text.class);
					
			return job.waitForCompletion(true)?0:1;*/
			Configuration conf = new Configuration();// 配置文件对象
			Path mypath = new Path(arg0[1]);
			FileSystem hdfs = mypath.getFileSystem(conf);// 创建输出路径
			if (hdfs.isDirectory(mypath)) {
				hdfs.delete(mypath, true);
			}

			Job job = new Job(conf, "tvplay");// 构造任务
			job.setJarByClass(TVPlay.class);// 设置主类

			job.setMapperClass(TVmapper.class);// 设置Mapper
			job.setMapOutputKeyClass(Text.class);// key输出类型
			job.setMapOutputValueClass(TVplaydata.class);// value输出类型
			job.setInputFormatClass(PlayinputFormat.class);//自定义输入格式

			job.setReducerClass(TVreducer.class);// 设置Reducer
			job.setOutputKeyClass(Text.class);// reduce key类型
			job.setOutputValueClass(Text.class);// reduce value类型
			// 自定义文件输出格式，通过路径名（pathname）来指定输出路径
			MultipleOutputs.addNamedOutput(job, "youku", TextOutputFormat.class,
					Text.class, Text.class);
			MultipleOutputs.addNamedOutput(job, "souhu", TextOutputFormat.class,
					Text.class, Text.class);
			MultipleOutputs.addNamedOutput(job, "tudou", TextOutputFormat.class,
					Text.class, Text.class);
			MultipleOutputs.addNamedOutput(job, "aiqiyi", TextOutputFormat.class,
					Text.class, Text.class);
			MultipleOutputs.addNamedOutput(job, "xunlei", TextOutputFormat.class,
					Text.class, Text.class);
			
			FileInputFormat.addInputPath(job, new Path(arg0[0]));// 输入路径
			FileOutputFormat.setOutputPath(job, new Path(arg0[1]));// 输出路径
			job.waitForCompletion(true);
			return 0;
		}
}




	





