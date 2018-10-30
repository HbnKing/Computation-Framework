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
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.hadoop.mapreduce.PlayinputFormat;
import com.hadoop.mapreduce.TVplaydata;

import teacher.tvplay.TVPlayCount;

/**
 * @input params 各类网站每天每部电视剧的点播量 收藏量 评论数 等数据的统计
 * @ouput params 分别输出每个网站 每部电视剧总的统计数据
 * @author yangjun
 * @function 自定义FileInputFormat 将电视剧的统计数据 根据不同网站以MultipleOutputs 输出到不同的文件夹下
 */
public class TVPlayCountCopy extends Configured implements Tool {
	/**
	 * @input Params Text TvPlayData
	 * @output Params Text TvPlayData
	 * @author yangjun
	 * @function 直接输出
	 */
	public static class TVPlayMapper extends
	 Mapper<Text, TVplaydata, Text, TVplaydata> {
		
		public void map(Text key,TVplaydata value, Context context) throws IOException, InterruptedException {
					context.write(key, value);
				}
		
		
	}
	/**
	 * @input Params Text TvPlayData
	 * @output Params Text Text
	 * @author yangjun
	 * @fuction 统计每部电视剧的 点播数 收藏数等  按source输出到不同文件夹下
	 */
	public static class TVPlayReducer extends
			Reducer<Text, TVplaydata, Text, Text> {
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
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();// 配置文件对象
		Path mypath = new Path(args[1]);
		FileSystem hdfs = mypath.getFileSystem(conf);// 创建输出路径
		if (hdfs.isDirectory(mypath)) {
			hdfs.delete(mypath, true);
		}

		Job job = new Job(conf, "tvplay");// 构造任务
		job.setJarByClass(TVPlayCountCopy.class);// 设置主类

		job.setMapperClass(TVPlayMapper.class);// 设置Mapper
		job.setMapOutputKeyClass(Text.class);// key输出类型
		job.setMapOutputValueClass(TVplaydata.class);// value输出类型
		job.setInputFormatClass(PlayinputFormat.class);//自定义输入格式

		job.setReducerClass(TVPlayReducer.class);// 设置Reducer
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
		
		FileInputFormat.addInputPath(job, new Path(args[0]));// 输入路径
		FileOutputFormat.setOutputPath(job, new Path(args[1]));// 输出路径
		job.waitForCompletion(true);
		return 0;
	}
	
		public static void main(String[] args) throws Exception {
			String[] paths = {"hdfs://wang:9000/mapreduce/tvplay.txt","hdfs://wang:9000/mapreduce/tvout/"};
			int ec = ToolRunner.run(new Configuration(), new TVPlayCountCopy(), paths);
			System.exit(ec);
	//public static int run(Configuration conf,Tool tool, String[] args),可以在job运行的时候指定配置文件或其他参数
	//这个方法调用tool的run(String[])方法，并使用conf中的参数，以及args中的参数，而args一般来源于命令行。
	
	}
}
