package org.mapreduce.customize;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
/**
* 学生成绩统计Hadoop程序
* 数据格式参考：19020090017 小讲 90 99 100 89 95
* @author HuangBQ
*/
public class ScoreCount extends Configured implements Tool {
    public static class ScoreMapper extends Mapper< Text, ScoreWritable, Text, ScoreWritable > {
        @Override
        protected void map(Text key, ScoreWritable value, Context context)
                throws IOException, InterruptedException {
            context.write(key, value);
        }
    }
    
    public static class ScoreReducer extends Reducer< Text, ScoreWritable, Text, Text > {
        private Text text = new Text();
        protected void reduce(Text Key, Iterable< ScoreWritable > Values, Context context)
                throws IOException, InterruptedException {
            float totalScore=0.0f;
            float averageScore = 0.0f;
            for(ScoreWritable ss:Values){
                totalScore +=ss.getChinese()+ss.getMath()+ss.getEnglish()+ss.getPhysics()+ss.getChemistry();
                averageScore +=totalScore/5;
            }
            text.set(totalScore+"\t"+averageScore);
            context.write(Key, text);
        }
    }
    
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();//读取配置文件
        
        Path mypath = new Path(args[1]);
        FileSystem hdfs = mypath.getFileSystem(conf);//创建输出路径
        if (hdfs.isDirectory(mypath)) {
            hdfs.delete(mypath, true);
        }
        
        Job job = new Job(conf, "ScoreCount");//新建任务
        job.setJarByClass(ScoreCount.class);//设置主类
        
        FileInputFormat.addInputPath(job, new Path(args[0]));// 输入路径
        FileOutputFormat.setOutputPath(job, new Path(args[1]));// 输出路径
        
        job.setMapperClass(ScoreMapper.class);// Mapper
        job.setReducerClass(ScoreReducer.class);// Reducer
        
        job.setMapOutputKeyClass(Text.class);// Mapper key输出类型
        job.setMapOutputValueClass(ScoreWritable.class);// Mapper value输出类型
                
        job.setInputFormatClass(ScoreInputFormat.class);//设置自定义输入格式
        
        job.waitForCompletion(true);        
        return 0;
    }
    
    public static void main(String[] args) throws Exception {
        String[] args0 = { 
        		"hdfs://single.hadoop.dajiangtai.com:9000/junior/score.txt",
                "hdfs://single.hadoop.dajiangtai.com:9000/junior/score-out/" 
                };
        int ec = ToolRunner.run(new Configuration(), new ScoreCount(), args0);
        System.exit(ec);
    }
}