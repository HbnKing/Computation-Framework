package com.mhdld.haoopjoin;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.FileSystem;



//利用分布式缓存  练习矩阵相乘
/*
 * 将两个矩阵相乘
 * 
 */


public class CaseTable2Track extends  Configured implements Tool  {
	
	public static class  MyMapper extends Mapper<LongWritable, Text, Text, Text>{
		private  Text outkey = new Text();
		private  Text outvalue = new Text();
		
		private List<String> caheList= new ArrayList<String>();
		List<String> list = null;
		
		protected void setup(Context context) throws IOException, InterruptedException{
			super.setup(context);
			//通过流入流将全局缓存中的右侧矩阵 读入list<string>中
			
			FileReader fr = new FileReader("track");
			
			BufferedReader br = new BufferedReader(fr);
			
			
			String line = null;
			while ((line = br.readLine())!=null){
				String[] valueItems=StringUtils.split(line,"\001");
				if (valueItems.length!=10) {
				/*	//track 共计十列   分别为
					id              	                    
					keepersn            	                    
					citygrid            	             	                    
					tracktime           	             	                    
					coordx              	             	                    
					coordy              	            	                    
					workgridcode        	            	                    
					inserttime          	            	                    
					errordesc           	           	                    
					errorcode */
					return;
				}else if(valueItems[4].equals(0)||valueItems[5].equals(0)||valueItems[4]== null||valueItems[5]==null){
					return;
				}
				caheList.add(valueItems[0]+"\t"+valueItems[4]+","+valueItems[5]);
			}
			//释放资源
			fr.close();
			br.close();
			
		}
		
		/**
		 * 
		 */
		public  void map(LongWritable key,Text value, Context context) throws IOException, InterruptedException{
			list = new ArrayList<String>();
			
			//case的行读取    
			String lineString = value.toString().trim();
			 String [] cases_lines = lineString.split("\001");
			 
			if (cases_lines.length == 167) {
				
			String case_id = cases_lines[1];
			String cases_location_x = cases_lines[18];
			String cases_location_y = cases_lines[19];
			boolean flag =cases_location_x.equals("0")||cases_location_y.equals("0")||cases_location_y.equals("\\N")||cases_location_x.equals("\\N");
			if (flag ==false) {
				
			
			for(String line:caheList){
			//遍历track 表 
				//行
				String track_id = line.toString().split("\t")[0];
				//列_值  (数组)
				String[] track_location = line.toString().split("\t")[1].split(",");
			//举证两行相乘得到的结果 
				if (distance(cases_location_x,cases_location_y,track_location[0],track_location[1])) {
					list.add(track_id);
					/*
					list.add("\t");
					list.add(track_location[0]);
					list.add("\t");
					list.add(track_location[1]);
					list.add(",");*/
				}else {
					continue;
				}
					}
			outkey.set(case_id +"\t"+cases_location_x+"\t"+cases_location_y);
			outvalue.set(list.toString());
			//输出格式 key :行  value 列_值
			context.write(outkey, outvalue);
			}
			}
	} 
	}
	/**
	 * 一个简单的比较距离的方法
	 */
	
	static boolean distance(String case_x, String case_y,String track_x,String track_y){
		
		BigDecimal casef_x = new BigDecimal(Double.parseDouble(case_x));
		BigDecimal casef_y = new BigDecimal(Double.parseDouble(case_y));
		BigDecimal trackf_x = new BigDecimal( Double.parseDouble(track_x));
		BigDecimal trackf_y = new BigDecimal(Double.parseDouble(track_y));
		
		//(casef_x.subtract(trackf_x)).pow(2).add((casef_y.subtract(trackf_y)).pow(2))
		//double dislocal = Math.sqrt(((casef_x.subtract(trackf_x)).pow(2).add((casef_y.subtract(trackf_y)).pow(2))).doubleValue());
				
		double DIS = 500;
		
		if(Math.abs((casef_x.subtract(trackf_x)).doubleValue())>=DIS){
			return false;
		}else if(Math.abs((casef_y.subtract(trackf_y)).doubleValue())>=DIS){
			return false;
		}else if (Math.sqrt(((casef_x.subtract(trackf_x)).pow(2).add((casef_y.subtract(trackf_y)).pow(2))).doubleValue())>= DIS ) {
			return false;
			
		}else {
			return true;
		}
		
	}

	
	
	//reduce  将每一行的值拼接
	
	/*public static  class MyReduce extends Reducer<Text, Text, Text, Text>{
		private  Text outkey2 = new Text();
		private  Text outvalue2 = new Text();
		
		public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException{
			//将列转换为行  
			StringBuffer sBuffer = new StringBuffer();
			for(Text t:values){
				sBuffer.append(t+",");
			}
			String line = null ;
			//判断最后一个,号  抹掉
			
			if(sBuffer.toString().endsWith(",")){
				line = sBuffer.substring(0, sBuffer.length()-1);
			}
			outkey2.set(key);
			outvalue2.set(line);
			
			context.write(outkey2, outvalue2);
			
		}
	}*/
	
	
	
	
	public static void main(String[] args) throws Exception {
		
		
		int  ec = -1;
		ec= ToolRunner.run(new Configuration(),new CaseTable2Track(),args);
		if(ec ==1){
			System.out.println("运行成功");
		}else if(ec == -1){
			System.out.println("运行失败");
		}
		System.exit(ec);
	}
	
	
	//设置几个静态量
	private static	String inpath = "hdfs://192.168.0.30:8020/user/hive/warehouse/case_9/part-m-00000";
	private static	String outpath = "hdfs://192.168.0.30:8020/user/root/mhdld/wh_test/out18";
	//将第一步的输出作为第二部的全局缓存
	private static String cache = "hdfs://192.168.0.30:8020/user/hive/warehouse/keeptrack_9/part-m-00000";
	private static	String hdfs = "hdfs://192.168.0.30:8020";
	

	//设置run方法
	public int run(String[] args){
		try {
			Configuration conf = new Configuration();
			/*String hdfs = "hdfs://192.168.0.30:8020";
			String cache = args[2];
			String inpath = args[0];
			String outpath = args[1]*/;
			conf.set("fs.defaultFS", hdfs );
			
			//创建一个job 
			
				Job job = Job.getInstance(conf,"location");
				//添加分布式缓存文件
				job.addCacheArchive(new URI(cache +"#track"));
				
				
				//设置job
				job.setJarByClass(CaseTable2Track.class);
				job.setMapperClass(MyMapper.class);
				
				//设置mapper　输出类型 
				job.setMapOutputKeyClass(Text.class);
				job.setMapOutputValueClass(Text.class);
				//设置reducer输出类型
				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(Text.class);
				//设置输入和输出路径
				
				FileSystem fs = FileSystem.get(conf);
				Path inputPath  = new Path(inpath);
				if(fs.exists(inputPath )){
					FileInputFormat.addInputPath(job, inputPath );
				}
				Path  outputPath = new Path(outpath);
				fs.delete(outputPath,true);
				
				FileOutputFormat.setOutputPath(job, outputPath);
					return job.waitForCompletion(true)?1:-1;
				
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
		
			} catch (URISyntaxException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}return -1;

}
}
