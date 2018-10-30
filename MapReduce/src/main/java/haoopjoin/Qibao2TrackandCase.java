package com.mhdld.haoopjoin;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Date;

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




/*
 *七宝表里有相同的  路名   以及其路名转折点的的   location  坐标(x - y)
 *track 表内有各个落点的的坐标  
 *要求
 *---------------------
 *
 *方法  map  遍历  道路表 所有数据
 *每行读取  将 道路编号   作为key  写回     匹配   所有所有落点数据  添加到value中 写回
 *(一般这个坐标的点是按续排的, 也可以指定一个字段 添加到value中 作为排序的标志)
 *
 *reduce  相同的区名作为key 将写回的多个坐标点的value遍历拆分 得到坐标 形成多个线段 
 *reduce 阶段  将track 表 track id  以及坐标 加载到内存中
 *
 *同时讲case 表的caseid  case location   case time 读取到内存中
 *
 *
 *多次遍历后写回
 *
 *最后输出值  <可能部分没有结果>
 * 
 */


public class Qibao2TrackandCase extends  Configured implements Tool  {
	public static class MyMapper extends Mapper<LongWritable, Text, Text, Text>{
		private Text outkey = new Text();
		private Text outvalue = new Text();
		@Override
		public  void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException {
			String[] line  = value.toString().split("\t");
			if (line.length == 15){
				outkey.set(line[13]);
				outvalue.set(line[5]+","+line[11]+"\t"+line[12]);
				context.write(outkey, outvalue);
			}
		}
	}
	
	public static class MyReducer extends Reducer<Text, Text, Text,Text>{
		
		private List<String> caheRoadList= new ArrayList<String>();
		List<String > listoutkey  = null ;
		List <String >list = null;
		/**
		 * 一个将track  case 表加载的方法
		 */
		private List<String> caheTrackList= new ArrayList<String>();
		private List<String> caheCaseList= new ArrayList<String>();
		
		protected void setup(Context context) throws IOException, InterruptedException{
			super.setup(context);
			//通过流入流将全局缓存中的track 表    读入list<string>中
			
			FileReader frtrack = new FileReader("track");
			BufferedReader brtrack = new BufferedReader(frtrack);
			String line = null;
			
		//0--id ;4--coordx ;5--coordy  7--inserttime;
			while ((line = brtrack.readLine())!=null){
				String[] valueItems=StringUtils.split(line,"\001");
				if (valueItems.length!=10) {
					continue;
				}else if(valueItems[4].equals(0)||valueItems[4].equals("###")||valueItems[5].equals(0)||valueItems[5].equals("###")||valueItems[4]== null||valueItems[5]==null||valueItems[7].equals("###")){
					continue;
				}
				//只把track 表 的  id  和  坐标  加入list  
				caheTrackList.add(valueItems[0]+"\t"+valueItems[4]+","+valueItems[5]+"\t"+valueItems[7]);
			}
			//释放资源
			frtrack.close();
			brtrack.close();
			
			/**
			 * 加载case 表
			 * 
			 */
			
			FileReader frCase = new FileReader("case");
			BufferedReader brCase = new BufferedReader(frCase);
			String lineCase = null;
			//0--id ; 18 --coordx  ; 19 --coordy  ;23--createtime  ;24 ---dispathtime 
			while ((lineCase = brCase.readLine())!=null){
				String[] valueItems=StringUtils.split(lineCase,"\t");
				if (valueItems.length!=167) {
					continue;
				}else if(valueItems[18].equals(0)||valueItems[18].equals("###")||valueItems[19].equals(0)||valueItems[19].equals("###")||valueItems[18]== null||valueItems[19]==null||valueItems[24].equals("###")){
					continue;
				}
				//只把track 表 的  id  和  坐标  加入list  
				caheCaseList.add(valueItems[0]+"\t"+valueItems[18]+","+valueItems[19]+"\t"+valueItems[24]);
			}
			//释放资源
			frCase.close();
			brCase.close();
			
			
		}
		
		private  Text outkey = new Text();
		private  Text outvalue = new Text();
		/**
		 * reduce  处理数据
		 */
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			//使用一个集合来装取坐标
			
			listoutkey  = new ArrayList<String>();
			for (Text t : values) {
				String[]  valuesplit = t.toString().split(",");
				caheRoadList.add( valuesplit[1]);
				
			}
			
			
			/**
			 * 开启比较
			 * 返回string 
			 */
			String result = StringUtils.strip(comparedTheBlind(caheRoadList, caheTrackList, caheCaseList).toString(),"[]");
			
			
			outkey.set(key);
			outvalue.set(result);
			context.write(outkey, outvalue);
			
		}
		
		
		
		
		
		
		private List<String> comparedTheBlind( List< String> listRoad,List<String > caheTrackList,List<String> caheCaseList ) {
			// TODO Auto-generated method stub
			//分别调用  三个盲区比较方法
			//1.先调用是否为空间盲区  返回boolean  类型  传入的为 road 和  track  点
			/**
			 * 遍历listroad   生成Roadid 在reduce 端保存  这里生成多个line
			 */
			
			
			List<Point>  trackPointsList = new ArrayList<Point>();
			Track  trackPoints = new Track();
			SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");  
	        java.util.Date date = null;
			for (String strtrack : caheTrackList) {
				Double track_x = Double.parseDouble(strtrack.split("\t")[1].split(",")[0]);
				Double track_y = Double.parseDouble(strtrack.split("\t")[1].split(",")[0]);
				String  timestr  = strtrack.split("\t")[2].trim();
				if (timestr.length()>=19) {
					timestr = timestr.substring(0, 19);
				}
				try {
					date = format.parse(timestr);
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		        
				Point trackPoint = new Point(track_x,track_y,date);
				 trackPointsList.add(trackPoint);
			}
			trackPoints.points=trackPointsList;
			
			Road road = new Road();
			List<Point> roadLineList = new ArrayList<Point>();
			for(String strRoad:listRoad){
				double road_x = Double.parseDouble(strRoad.split("\t")[0].trim());
				double road_y = Double.parseDouble(strRoad.split("\t")[1].trim());
				Point point1 = new Point(road_x,road_y);
				roadLineList.add(point1);
			}
			road.points = roadLineList;
			
			
			
			Track accidentTrack  = new Track();
			
			List<Point> casePointList = new ArrayList<Point>();
				
			for (String strcase : caheCaseList) {
				Double case_x = Double.parseDouble(strcase.split("\t")[1].split(",")[0]);
				Double case_y = Double.parseDouble(strcase.split("\t")[1].split(",")[1]);
				//Point casePoint = new Point(case_x,case_y);
				String  timestr  = strcase.split("\t")[2].trim();
				if (timestr.length()>=19) {
					timestr=timestr.substring(0, 19);
				}
				try {
					date = format.parse(timestr);
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		        
				Point casePoint = new Point(case_x,case_y,date);
				
				casePointList.add(casePoint);
			}
			accidentTrack.points=casePointList;
			
			
			boolean flagSpace = isBlindRoadInSpace(road, trackPoints);
			boolean  flagSpacewithCase  = isBlindRoadSpaceWithCase(trackPoints, accidentTrack, road);
			boolean flagInTimeWithCase  = isBlindRoadInTimeWithCase(trackPoints, accidentTrack, road);
			List<String> blindtype  = new ArrayList<String>();
			if (flagInTimeWithCase) {
				blindtype.add("空间盲区");
			} 
			if (flagSpacewithCase) {
				blindtype.add("案件盲区");
			}
			if (flagSpace) {
				blindtype.add("时间盲区");
			}
			return blindtype;
			
		}


		/**
		 * 空间盲区算法  涉及  road  track
		 * @param track
		 * @param road
		 * @return
		 */
		public  boolean isBlindRoadInSpace(Road road,Track track){
			int argcount_line =20;//一条线段上点的个数
			double argrate_line = 0.6;//一条线段上有点部分的比例，越高，要求越严格，盲区越多
			double argrange_road = 0.6;//一条道路上有点路段的比例，越高，盲区越多
			int distance = 50;//道路的范围
			
			List<Point> trackPoints = track.points;
			
			//对每个路段进行判断的规则
			int countBlindRoad = 0;
			//Road roadtmp = road;
			int sum = road.getLines().size();
			List<Line>  lines = road.getLines();
			for (Line line2 : lines) {    ///*******
				int countPoints = countPointsOnLine(line2, trackPoints,distance);
				double rate = countPointDensityOfLine(line2, trackPoints,distance);
				
				//判定规则
				if(countPoints<argcount_line&&rate<argrate_line){
					line2.isBlind=true;
					countBlindRoad++;
				}
					
			}
			if((double)(sum-countBlindRoad)/sum < argrange_road)return true;
			
			return false;
			 
		}
		
		//案件盲区算法
		public boolean isBlindRoadSpaceWithCase(Track track,Track accident,Road road){
			List<Point> trackPoints = track.points;
			List<Point> casePoints = accident.points;
			
			int argcount_line =20;//一条线段上点的个数
			double argrate_line = 0.6;//一条线段上有点部分的比例，越高，要求越严格，盲区越多
			double argrange_road = 0.6;//一条道路上有点路段的比例，越高，盲区越多
			int distance = 50;//道路的范围
			int argcase_count = 2;//案件的个数阈值
			
			//对每个路段进行判断的规则
			int countBlindRoad = 0;
			int countaccident = 0;
			
			
			int sum = road.getLines().size();
			List<Line>  lines = road.getLines();

			for (Line line2 : lines) {
				int countPoints = countPointsOnLine(line2, trackPoints,distance);
				double rate = countPointDensityOfLine(line2, trackPoints,distance);
				//查看有没有案件
				countaccident += countAccident(line2, casePoints,distance);
				
				//判定规则
				if(countPoints<argcount_line&&rate<argrate_line){
					line2.isBlind=true;
					countBlindRoad++;
				}
					
			}
			
			if((double)(sum-countBlindRoad)/sum < argrange_road&&countaccident>argcase_count)return true;
			
			return false;
		}
		
		//时间盲区算法
		public  boolean isBlindRoadInTimeWithCase(Track track,Track accident,Road road){
			int  argcase_count = 1;
			int  distance = 50;
			
			Track tmorning = TrackUtil.getMorningTrack(track);
			Track tafter = TrackUtil.getAfternoonTrack(track);
			Track accidmoring = TrackUtil.getMorningTrack(accident);
			Track accidafter = TrackUtil.getAfternoonTrack(accident);
			
			if(!isBlindRoadInSpace(road, tmorning) && hasAccident(road, accidmoring.points, distance)<argcase_count){
				if(isBlindRoadInSpace(road, tafter) && hasAccident(road, accidafter.points, distance)>=argcase_count)
					return true;
			}
			
			
			return false;
		}
		
		
		class Road {
			public List<Point> points;

			public Road() {
				super();
				// TODO Auto-generated constructor stub
			}
			
			public List<Line> getLines(){
				
				Point s = points.get(0);
				int tag = 0;
				List<Line> lines = new ArrayList<Line>();
				
				for(Point p : points ){
					if(tag!=0){
						s = points.get(tag-1);
						Line l = new Line(s, p);
						lines.add(l);
					}
					tag++;
					
				}
				
				return lines;
			}
		}

		
		
		public  static class Track {
			public List<Point> points;
		}
		
		 class Point {
			
			public double x;
			public double y;
			public Date  date;
			
			public Point(){
				this.x=0;
				this.y=0;
			}
			public Point(double x,double y){
				this.x=x;
				this.y=y;
			}
			public Point(double x,double y,Date date2){
				this.x=x;
				this.y=y;
				this.date = date2;
			}
			
			public double Distance(Point p){
				double linelength =0;
				linelength = Math.sqrt(
						(this.x-p.x)*(this.x-p.x)+
						(this.y-p.y)*(this.y-p.y)
						);
				return linelength;
			}
			public void setX(Double x) {
				// TODO Auto-generated method stub
				this.x=x;
			}
			public void setY(Double y) {
				// TODO Auto-generated method stub
				this.y=y;
			}
		}
		 
		 
		//----------------------------------------------------------
				//获取线段的长度
				public double lineSpace(Point x1,Point x2){
					double length =0;
					length = Math.sqrt(
							(x1.x-x2.x)*(x1.x-x2.x)+
							(x1.y-x2.y)*(x1.y-x2.y)
							);
					return length;
				}
				//计算点到线段的距离   
				public  double getDistanceToLine(Point x,Line line){
					double length = 0;
					double len,a,b;
					//计算线段长度  调用上面的lineSpace
					len=lineSpace(line.start,line.end);
					//点x 到起始点start的距离
					a=lineSpace(x, line.start);
					//点x 到起始点end的距离
					b = lineSpace(x, line.end);
					if(len<0.00001){
						return 0;
					}
					if(a<0.0001||b<0.0001)return 0;
					
					if(b*b >= a*a + len*len)return a;
					if(a*a >= b*b + len*len)return b;
					
					double p = (len+a+b)/2;
					double s = Math.sqrt(p * (p - a) * (p - b) * (p - len));
					length = 2 * s / len;
					
					//返回点到线段距离   
					return length;
				}
				
				public  boolean isPointOnLine(Point x,Line line,int distance){
					
					if(getDistanceToLine(x, line)>distance)
//					if(getDistanceToLine(x, line)>line.getLength()*0.2)
						return false;
					return true;
				}
				
				public  boolean isPointOnSeg(Point x,Line line,int distance){
					double length = 0;
					double len,a,b;
					len=lineSpace(line.start,line.end);
					a=lineSpace(x, line.start);
					b = lineSpace(x, line.end);
					if(len<0.00001){
						return false;
					}
					if(a<0.0001||b<0.0001)return false;
					
					if(b*b >= a*a + len*len)return false;
					if(a*a >= b*b + len*len)return false;
					
					double p = (len+a+b)/2;
					double s = Math.sqrt(p * (p - a) * (p - b) * (p - len));
					length = 2 * s / len;
					
					if(length<distance)return true;
					
					return false;
				}
				public  int  countPointsOnLine(Line line,List<Point> points,int distance){
					int shuffledpointsSum = 0;
					for (Point point : points) {
						if(isPointOnLine(point, line,distance)){
							shuffledpointsSum++;
						}
					}
					return shuffledpointsSum;
				}
				
				public  int countPointsOnSeg(Line line,List<Point> points,int distance){
					int count = 0;
					for (Point point : points) {
						if(isPointOnSeg(point, line,distance)){
							count++;
						}
					}
					return count;
				}
				
				public  int shufflePointsWithFuzzy(Point x,Line line){
					int shuffledpointsSum = 0;
					//TODO
					return shuffledpointsSum;
				}
				
				//返回一条线段上以20米为一段，有点的段的占比
				public  double countPointDensityOfLine(Line line,List<Point> points,int distance){
					int countdensity=0;
					Point start = line.start;
					Point end = line.end;
					
					double stepx = (end.x-start.x)/line.getLength();//对应x方向一步
					double stepy = (end.y-start.y)/line.getLength();//对应y方向一步
					
					Point s=start;
					Point e =null;
					//以20米为一步来切分线段，统计没有点的线段个数
					for(double step=20.0;step<line.getLength();step=step+20){
						 e = new Point(s.x+stepx*step,s.y+stepy*step);
						//e = new Point(s.x+stepx*step,s.y+stepy*step);
						Line l = new Line(s, e);
						if (countPointsOnSeg(l, points,distance)>1) {
							countdensity++;
						}
						s=e;
						
					}
					e = end;
					Line l = new Line(s, e);
					if (countPointsOnSeg(l, points,distance)>1) {
						countdensity++;
					}
					
					return countdensity/(line.getLength()/20+1);
				}
				//--------------------------------------------------------
				public int countAccident(Line line,List<Point> points,int distance){
					int shuffledpointsSum = 0;
					for (Point point : points) {
						if(isPointOnLine(point, line,distance)){
							shuffledpointsSum++;
						}
					}
					return shuffledpointsSum;
				}
				
				public  int hasAccident(Road road,List<Point> points,int distance){
					int result =0;
					List<Line>  lines = road.getLines();
					for (Line line : lines) {
						result += countAccident(line, points, distance);
					}
					
					return result;
				}
	//--------------------------------------------------------------
				static class TrackUtil {
					
					public static  Track getMorningTrack(Track track){
						Track t = new Track();
						List<Point> plist = new ArrayList<>();
						for(Point p : track.points){
							int hour =  p.date.getHours();
							if(hour>=6 && hour<=12)
							{
								plist.add(p);
							}
						}
						t.points = plist;
						return t;
					}
					
					public static Track getAfternoonTrack(Track track){
						Track t = new Track();
						List<Point> plist = new ArrayList<>();
						for(Point p : track.points){
							int hour =  p.date.getHours(); 
							if(hour>12 && hour<=20)
							{
								plist.add(p);
							}
						}
						t.points = plist;
						return t;
					}
				}

				//--------------------------------------------------------------------

		
		  class Line {
			public Point start;
			public Point end;
			public boolean isBlind;
			
			public Line() {
				super();
				// TODO Auto-generated constructor stub
			}
			public Line(Point start, Point end) {
				super();
				this.start = start;
				this.end = end;
				this.isBlind = false;
			}
			
			public double getLength(){
				return Math.sqrt(
						(start.x-end.x)*(start.x-end.x)+
						(start.y-end.y)*(start.y-end.y)
						);
			}
			
			
		}

		
	}
	
 	
	
	
	
	
	public static void main(String[] args) throws Exception {
		int  ec = -1;
		ec= ToolRunner.run(new Configuration(),new Qibao2TrackandCase(),args);
		if(ec ==1){
			System.out.println("运行成功");
		}else if(ec == -1){
			System.out.println("运行失败");
		}
		System.exit(ec);
	}
	
	
	//设置几个静态量
	private static	String inpath = "hdfs://192.168.0.30:8020/user/root/mhdld/sqoop/qibaoroad/part-m-00000";
	private static	String outpath = "hdfs://192.168.0.30:8020/user/root/mhdld/sqoop/testout";
	//将第一步的输出作为第二部的全局缓存
	private static	String cachecase = "hdfs://192.168.0.30:8020/user/root/mhdld/sqoop/case/part-m-00000";
	private static	String cachetrack = "hdfs://192.168.0.30:8020/user/root/mhdld/sqoop/track/part-m-00000";
	private static	String hdfs = "hdfs://192.168.0.30:8020";

	//设置run方法
	public int run(String[] args){
		try {
			Configuration conf = new Configuration();
			
			conf.set("fs.defaultFS", hdfs );
			
			//创建一个job 
			
				Job job = Job.getInstance(conf,"location");
				//添加分布式缓存文件
				job.addCacheArchive(new URI(cachecase +"#case"));
				job.addCacheArchive(new URI(cachetrack+"#track"));
				
				
				//设置job
				job.setJarByClass(Qibao2TrackandCase.class);
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

	
	
