package org.mapreduce.examples;

import java.io.IOException;
import java.util.Arrays;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
public class Anagram extends Configured implements Tool{
	
	public static  class AnagramMapper extends Mapper< Object, Text, Text, Text> {

	    private Text sortedText = new Text();
	    private Text orginalText = new Text();

	    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

	        String word = value.toString();
	        char[] wordChars = word.toCharArray();//单词转化为字符数组
	        Arrays.sort(wordChars);//对字符数组按字母排序
	        String sortedWord = new String(wordChars);//字符数组转化为字符串
	        sortedText.set(sortedWord);//设置输出key的值
	        orginalText.set(word);//设置输出value的值
	        context.write( sortedText, orginalText );//map输出
	    }

	}
	
	
	
	public static class AnagramReducer extends Reducer< Text, Text, Text, Text> {
		   
	    private Text outputKey = new Text();
	    private Text outputValue = new Text();

	   
	    public void reduce(Text anagramKey, Iterable< Text> anagramValues,
	            Context context) throws IOException, InterruptedException {
	            String output = "";
	            //对相同字母组成的单词，使用 ~ 符号进行拼接
	            for(Text anagam:anagramValues){
	            	 if(!output.equals("")){
	            		 output = output + "~" ;
	            	 }
	            	 output = output + anagam.toString() ;
	            }
	            StringTokenizer outputTokenizer = new StringTokenizer(output,"~" );
	            //输出anagrams（字谜）大于2的结果
	            if(outputTokenizer.countTokens()>=2)
	            {
	                    output = output.replace( "~", ",");
	                    outputKey.set(anagramKey.toString());//设置key的值
	                    outputValue.set(output);//设置value的值
	                    context.write( outputKey, outputValue);//reduce
	            }
	    }

	}
    
    @SuppressWarnings( "deprecation")
    @Override
    public  int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        
        //删除已经存在的输出目录
        Path mypath = new Path(args[1]);
        FileSystem hdfs = mypath.getFileSystem(conf);
         if (hdfs.isDirectory(mypath)) {
            hdfs.delete(mypath, true);
        }
        Job job = new Job(conf, "testAnagram");
        job.setJarByClass(Anagram. class);	//设置主类
        
        job.setMapperClass(AnagramMapper. class);	//Mapper
        job.setMapOutputKeyClass(Text. class);
        job.setMapOutputValueClass(Text. class);
        job.setReducerClass(AnagramReducer. class);	//Reducer
        job.setOutputKeyClass(Text. class);
        job.setOutputValueClass(Text. class);
        FileInputFormat.addInputPath(job, new Path(args[0]));	//设置输入路径
        FileOutputFormat. setOutputPath(job, new Path(args[1]));	//设置输出路径
        return job.waitForCompletion(true) ? 0 : 1;//提交作业
        
    }

    public static void main(String[] args) throws Exception{
		//数据的输入路径和输出路径
        String[] args0 = { "hdfs://dajiangtai:9000/anagram/anagram.txt" ,
        "hdfs://dajiangtai:9000/anagram/output"};
        int ec = ToolRunner.run( new Configuration(), new Anagram(), args0);
        System. exit(ec);
    }
}