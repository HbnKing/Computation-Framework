package org.mapreduce.customize;

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
/**
 * 
 * @author yangjun
 * @function key vlaue 输入格式
 */
public class TVPlayInputFormat extends FileInputFormat<Text,TVPlayData>{

	@Override
	public RecordReader<Text, TVPlayData> createRecordReader(InputSplit input,
			TaskAttemptContext context) throws IOException, InterruptedException {
		return new TVPlayRecordReader();
	}

	public class TVPlayRecordReader extends RecordReader<Text, TVPlayData>{
		public LineReader in;  
	    public Text lineKey; 
	    public TVPlayData lineValue;
	    public Text line;
		@Override
		public void close() throws IOException {
			if(in !=null){
				in.close();
			}
		}

		@Override
		public Text getCurrentKey() throws IOException, InterruptedException {
			return lineKey;
		}

		@Override
		public TVPlayData getCurrentValue() throws IOException, InterruptedException {
			return lineValue;
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			return 0;
		}

		@Override
		public void initialize(InputSplit input, TaskAttemptContext context)
				throws IOException, InterruptedException {
			FileSplit split=(FileSplit)input;  
	        Configuration job=context.getConfiguration();  
	        Path file=split.getPath();  
	        FileSystem fs=file.getFileSystem(job);  
	          
	        FSDataInputStream filein=fs.open(file); 
	        in=new LineReader(filein,job); 
	        line=new Text();  
	        lineKey=new Text();
	        lineValue = new TVPlayData();
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			int linesize=in.readLine(line); 
			if(linesize==0)  return false; 
			String[] pieces = line.toString().split("\t"); 
	        if(pieces.length != 7){  
	            throw new IOException("Invalid record received");  
	        }
	        lineKey.set(pieces[0]+"\t"+pieces[1]);
	        lineValue.set(Integer.parseInt(pieces[2]),Integer.parseInt(pieces[3]),Integer.parseInt(pieces[4])
	        		,Integer.parseInt(pieces[5]),Integer.parseInt(pieces[6]));
			return true;
		}
	}
}
