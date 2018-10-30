package com.hadoop.mapreduce;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;


//定义一个tvplayinputformat 类  来继承fileinputformat .并重写内部RecordReader 方法
//两个重要方法  1.获取分片  2.获取key -value 对	
public class PlayinputFormat extends FileInputFormat<Text, TVplaydata>{

	@Override
	public RecordReader<Text, TVplaydata> createRecordReader(InputSplit input, TaskAttemptContext context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
		return new TvplayRecordReader();
	}
	class TvplayRecordReader  extends RecordReader<Text, TVplaydata>{
		
		public LineReader in;  
	    public Text lineKey; 
	    public TVplaydata lineValue;
	    public Text line;
		
		@Override
		public void close() throws IOException {
			// TODO Auto-generated method stub
			if(in !=null){
				in.close();
			}
		}

		@Override
		public Text getCurrentKey() throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			return lineKey;
		}

		@Override
		public TVplaydata getCurrentValue() throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			return lineValue;
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public void initialize(InputSplit input, TaskAttemptContext context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			FileSplit split=(FileSplit)input;  
	        Configuration job=context.getConfiguration();  
	        Path file=split.getPath();  
	        FileSystem fs=file.getFileSystem(job);  
	          
	        FSDataInputStream filein=fs.open(file); //打开文件
	        in=new LineReader(filein,job); 
	        line=new Text();  
	        lineKey=new Text(); //新建一个text实例作为自定义格式输入的key
	        lineValue = new TVplaydata(); //新建一个tvplaydata 实例作为自动以格式输入的value
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			int linesize=in.readLine(line); 
			if(linesize==0)  return false; 
			//读取每行数据并转化为数组
			String[] pieces = line.toString().split("\t"); 
	        if(pieces.length != 7){  
	            throw new IOException("Invalid record received");  
	        }
	        
	       //自定义key 值和value值
	        lineKey.set(pieces[0]+"\t"+pieces[1]);
	        lineValue.set(Integer.parseInt(pieces[2]),Integer.parseInt(pieces[3]),Integer.parseInt(pieces[4])
	        		,Integer.parseInt(pieces[5]),Integer.parseInt(pieces[6]));
			return true;
		}
	}
 }
		
	