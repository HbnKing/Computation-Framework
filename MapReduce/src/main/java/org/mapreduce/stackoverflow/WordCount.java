package org.mapreduce.stackoverflow;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler.RandomSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
public class WordCount {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            
            while (itr.hasMoreTokens()) {
            	String a = itr.nextToken();
               // word.set(itr.nextToken());
               // context.write(word, one);
            	context.write(new Text(a), one);
            }
        }
    }

    public static class IntSumReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int count = 0;
            for (IntWritable val : values) {
                count += val.get();
            }
            result.set(count);
            context.write(key, result);

        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");

        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setNumReduceTasks(100);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path("hdfs://192.168.0.30:8020/user/wh/weather/30yr_03103.dat"
				));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.0.30:8020/user/wh/weather/out3/"));

        // total order part
        Path partitionFile = new Path("partitionFile");
        RandomSampler<Text, Text> sampler = new RandomSampler<Text, Text>(0.1, 10000, 10);
        TotalOrderPartitioner.setPartitionFile(job.getConfiguration(), partitionFile);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setPartitionerClass(TotalOrderPartitioner.class);
        InputSampler.writePartitionFile(job, sampler);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}