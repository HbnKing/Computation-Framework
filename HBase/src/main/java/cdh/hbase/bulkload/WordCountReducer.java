package cdh.hbase.bulkload;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>  
{  

    private IntWritable result=new IntWritable();  
    protected void reduce(Text key, Iterable<IntWritable> valueList,  
            Context context)  
            throws IOException, InterruptedException {  
        // TODO Auto-generated method stub  
        int sum=0;  
        for(IntWritable value:valueList)  
        {  
            sum+=value.get();  
        }  
        result.set(sum);  
        context.write(key, result);  
    }  
      
}  