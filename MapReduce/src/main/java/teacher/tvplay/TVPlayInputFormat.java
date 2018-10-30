package teacher.tvplay;

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
		//初始化的方法
		public void initialize(InputSplit input, TaskAttemptContext context)
				throws IOException, InterruptedException {
			FileSplit split=(FileSplit)input;  
	        Configuration job=context.getConfiguration();  
	        Path file=split.getPath();  
	        FileSystem fs=file.getFileSystem(job);  
	          
	        FSDataInputStream filein=fs.open(file); //打开文件
	        in=new LineReader(filein,job); 
	        line=new Text();  
	        lineKey=new Text(); //新建一个text实例作为自定义格式输入的key
	        lineValue = new TVPlayData(); //新建一个tvplaydata 实例作为自动以格式输入的value
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
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
