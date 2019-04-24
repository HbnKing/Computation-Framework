package org.mapreduce.customize;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class SecondarySort{
    // 自定义map
    public static class Map extends Mapper<LongWritable, Text, IntPair, IntWritable>{
        private final IntPair intkey = new IntPair();
        private final IntWritable intvalue = new IntWritable();
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            int left = 0;
            int right = 0;
            if (tokenizer.hasMoreTokens()){
                left = Integer.parseInt(tokenizer.nextToken());
                if (tokenizer.hasMoreTokens())
                    right = Integer.parseInt(tokenizer.nextToken());
                intkey.set(left, right);
                intvalue.set(right);
                context.write(intkey, intvalue);
            }
        }
    }
    // 自定义reduce
    public static class Reduce extends Reducer< IntPair, IntWritable, Text, IntWritable>{
        private final Text left = new Text();      
        public void reduce(IntPair key, Iterable< IntWritable> values,Context context) throws IOException, InterruptedException{
            left.set(Integer.toString(key.getFirst()));
            for (IntWritable val : values){
                context.write(left, val);
            }
        }
    }
    /**
     * @param args
     */
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException{
        // TODO Auto-generated method stub
        Configuration conf = new Configuration();

        Job job = new Job(conf, "secondarysort");
        job.setJarByClass(SecondarySort.class);
        
        FileInputFormat.setInputPaths(job, new Path(args[0]));//输入路径
        FileOutputFormat.setOutputPath(job, new Path(args[1]));//输出路径

        job.setMapperClass(Map.class);// Mapper
        job.setReducerClass(Reduce.class);// Reducer
        
        job.setPartitionerClass(FirstPartitioner.class);// 分区函数
        //job.setSortComparatorClass(KeyComparator.Class);//本课程并没有自定义SortComparator，而是使用IntPair自带的排序
        job.setGroupingComparatorClass(GroupingComparator.class);// 分组函数


        job.setMapOutputKeyClass(IntPair.class);
        job.setMapOutputValueClass(IntWritable.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
       
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}