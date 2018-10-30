package com.mhdld.haoopjoin;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;


/**
 * 案件表一张 
 * 落点表一张
 * 获取案件表的案件id  x 和y 坐标
 * 获取落点边的落点编号   x 和y 坐标
 * 
 * 以案件id ?为主键   将 案件x 案件y 提取出来 
 * 比较当前案件 左边和 落点坐标   进行比较位置信息
 * 在这个范围内 将落点的id  列出
 * 
 * 
 * 
 * @author Administrator
 *
 */

public class AnyCaseTable {
	
	public static class MyMapper extends Mapper<LongWritable, Text, Text, Text>{
		Text text = new Text();
		@Override
		public void map (LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			String lineString = value.toString().trim();
			 String [] lines = lineString.split("\001");
			 int a = lines.length;
			if(a==167){
			text.set(lines[18]+"\t" +lines[19]);
			context.write(new Text(lines[1]), text);
			}
		}
		
	}
/*	public static class MyReducer extends Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>{
		
	}*/
	
	

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		String[] args0 =  {"hdfs://hadoop1:8020/user/hive//warehouse/case_9/part-m-00000",
      "hdfs://hadoop1:8020/user/root/mhdld/wh_test/out"};
		Configuration conf = new Configuration();
		
		 Path mypath = new Path(args0[1]);//输出路径
		    FileSystem hdfs = mypath.getFileSystem(conf);//获取文件系统
		    
		  //如果文件系统中存在这个输出路径，则删除掉，保证输出目录不能提前存在。
		  		if (hdfs.isDirectory(mypath)) {
		  			hdfs.delete(mypath, true);
		  		}
		Job job = new Job(conf ,"count");
		
		job.setJarByClass(CaseTable2Track.class);
		job.setMapperClass(MyMapper.class);
  		job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text. class);
        job.setInputFormatClass(TextInputFormat.class);
       
  		
  		
  		FileInputFormat.setInputPaths(job, new Path(args0[0]));//FileInputFormat.addInputPath（）指定的这个路径可以是单个文件、一个目录或符合特定文件模式的一系列文件。
  		FileOutputFormat.setOutputPath(job, new Path(args0[1]));
  		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	

	}
	
	

}
