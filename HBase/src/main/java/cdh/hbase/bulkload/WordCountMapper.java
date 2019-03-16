package cdh.hbase.bulkload;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>  
{  

    private Text wordText=new Text();  
    private IntWritable one=new IntWritable(1);  
    @Override  
    protected void map(LongWritable key, Text value, Context context)  
            throws IOException, InterruptedException {  
        // TODO Auto-generated method stub  
        String line=value.toString();  
        String[] wordArray=line.split(" ");  
        for(String word:wordArray)  
        {  
            wordText.set(word);  
            context.write(wordText, one);  
        }  
          
    }  
}  