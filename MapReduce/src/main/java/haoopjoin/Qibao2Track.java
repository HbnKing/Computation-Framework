package com.mhdld.haoopjoin;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
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
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.FileSystem;
import com.mhdld.model.Point;


/*
 *七宝表里有相同的  路名   以及其路名转折点的的   location  坐标(x - y)
 *track 表内有各个落点的的坐标  
 *要求
 *找出一个区名内的所有   坐标点
 *
 *方法  map  遍历 所有数据
 *每行读取  将 道路编号   作为key  写回     匹配   所有所有落点数据  添加到value中 写回
 *(一般这个坐标的点是按续排的, 也可以指定一个字段 添加到value中 作为排序的标志)
 *
 *reduce  相同的区名作为key   将写回的多个坐标点的value遍历拆分 得到坐标 形成多个线段 
 *reduce 阶段  将track 表 track id  以及坐标 加载到内存中
 *多次遍历后写回
 *
 *最后输出值  <可能部分没有结果>
 * 
 */


public class Qibao2Track extends  Configured implements Tool  {
	public static class MyMapper extends Mapper<LongWritable, Text, Text, Text>{
		private Text outkey = new Text();
		private Text outvalue = new Text();
		@Override
		public  void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException {
			String[] line  = value.toString().split("\001");
			if (line.length == 15){
				outkey.set(line[13]);
				outvalue.set(line[5]+","+line[11]+"\t"+line[12]);
				context.write(outkey, outvalue);
			}
		}
	}
	
	public static class MyReducer extends Reducer<Text, Text, Text,Text>{
		/**
		 * 一个将track 表加载的方法
		 */
		private List<String> caheList= new ArrayList<String>();
		List<String > listoutkey  = null ;
		List <String >list = null;
		
		protected void setup(Context context) throws IOException, InterruptedException{
			super.setup(context);
			//通过流入流将全局缓存中的track 表    读入list<string>中
			
			FileReader fr = new FileReader("track");
			
			BufferedReader br = new BufferedReader(fr);
			String line = null;
			while ((line = br.readLine())!=null){
				String[] valueItems=StringUtils.split(line,"\001");
				if (valueItems.length!=10) {
					continue;
				}else if(valueItems[4].equals(0)||valueItems[5].equals(0)||valueItems[4]== null||valueItems[5]==null){
					continue;
				}
				//只把track 表 的  id  和  坐标  加入list  
				caheList.add(valueItems[0]+"\t"+valueItems[4]+","+valueItems[5]);
			}
			//释放资源
			fr.close();
			br.close();
		}
		
		private  Text outkey = new Text();
		private  Text outvalue = new Text();
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			//使用一个集合来装取坐标
			list = new ArrayList<String>();
			listoutkey  = new ArrayList<String>();
			for (Text t : values) {
				String[]  valuesplit = t.toString().split(",");
				list.add( valuesplit[1]);
				
			}
			
			List<String> list2 = new ArrayList<String>();
			for (int i=1 ;i<list.size();i++) {
				list2.add(list.get(i-1)+","+list.get(i));
				
			}
			
			for (String string : list2) {
			
				//再将track  表加入  比较距离
				//遍历track表 内容  
				for (String tracklines : caheList) {
					String track_id= tracklines.split("\t")[0];
					String track_location = tracklines.split("\t")[1];
					if ((listoutkey.contains(track_id)==false)&&distance(string ,track_location)) {
						
						listoutkey.add(track_id);
						
					}else{
						continue;
					}
				}
				
			}
			
			outkey.set(key);
			outvalue.set(listoutkey.toString());
			context.write(outkey, outvalue);
			
		}
		private boolean distance(String string, String track_location) {
			// TODO Auto-generated method stub
			/**
			 * 计算track location 到 qibaoroad 的距离
			 */
			Double road_a_x = Double.parseDouble(string.split(",")[0].split("\t")[0]);
			Double road_a_y = Double.parseDouble(string.split(",")[0].split("\t")[1]);
			Double road_b_x = Double.parseDouble(string.split(",")[1].split("\t")[0]);
			Double road_b_y = Double.parseDouble(string.split(",")[1].split("\t")[1]);
			Double track_x = Double.parseDouble(track_location.split(",")[0]);
			Double track_y = Double.parseDouble(track_location.split(",")[1]);
			
			
			Double DISTANCE = 50d;
			//Double K = (road_b_y- road_a_y)/(road_b_x-road_a_x);
			Double dis = pointToLine(road_a_x,road_a_y,road_b_x,road_b_y,track_x,track_y);
			if (dis <= DISTANCE  ) {
			return true;
			} else {
			return false;

			}
			
		}
	       private double pointToLine(double x1, double y1, double x2, double y2, double x0,
	    		   double y0) {
	            double space = 0;
	            double a, b, c;
	            
	           //计算各个线段的长度
	            a = lineSpace(x1, y1, x2, y2);// 线段的长度
	            b = lineSpace(x1, y1, x0, y0);// (x1,y1)到点的距离
	            c = lineSpace(x2, y2, x0, y0);// (x2,y2)到点的距离
	            if (c <= 0.000001 || b <= 0.000001) {
	               space = 0;
	               return space;
	            }
	            if (a <= 0.000001) {
	               space = b;
	               return space;
	            }
	            if (c * c >= a * a + b * b) {
	               space = b;
	               return space;
	            }
	            if (b * b >= a * a + c * c) {
	               space = c;
	               return space;
	            }
	            double p = (a + b + c) / 2;// 半周长
	            double s = Math.sqrt(p * (p - a) * (p - b) * (p - c));// 海伦公式求面积
	            space = 2 * s / a;// 返回点到线的距离（利用三角形面积公式求高）
	            return space;
	        }
	        // 计算两点之间的距离
	        private double lineSpace(double x1, double y1, double x2, double y2) {
	            double lineLength = 0;
	            lineLength = Math.sqrt((x1 - x2) * (x1 - x2) + (y1 - y2)* (y1 - y2));
	            return lineLength;
	            
	        }
		
	}
	
 	
	
	
	
	
	public static void main(String[] args) throws Exception {
		int  ec = -1;
		ec= ToolRunner.run(new Configuration(),new Qibao2Track(),args);
		if(ec ==1){
			System.out.println("运行成功");
		}else if(ec == -1){
			System.out.println("运行失败");
		}
		System.exit(ec);
	}
	
	
	//设置几个静态量
	private static	String inpath = "hdfs://192.168.0.30:8020/user/hive/warehouse/qibaoroadnode/part-m-00000";
	private static	String outpath = "hdfs://192.168.0.30:8020/user/root/mhdld/wh_test/ecqibaoroad";
	//将第一步的输出作为第二部的全局缓存
	private static String cache = "hdfs://192.168.0.30:8020/user/hive/warehouse/keeptrack_9/part-m-00000";
	private static	String hdfs = "hdfs://192.168.0.30:8020";

	//设置run方法
	public int run(String[] args){
		try {
			Configuration conf = new Configuration();
			
			conf.set("fs.defaultFS", hdfs );
			
			//创建一个job 
			
				Job job = Job.getInstance(conf,"location");
				//添加分布式缓存文件
				job.addCacheArchive(new URI(cache +"#track"));
				
				
				//设置job
				job.setJarByClass(Qibao2Track.class);
				job.setMapperClass(MyMapper.class);
				job.setReducerClass(MyReducer.class);
				//job.setNumReduceTasks(10);
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

	
	
