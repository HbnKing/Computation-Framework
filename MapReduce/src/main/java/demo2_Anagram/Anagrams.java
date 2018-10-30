package com.work.hadoopwordcount;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
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

public class Anagrams extends Configured implements Tool {
	//дMap����
	public static class Anagramsmapper extends Mapper<LongWritable, Text, Text, Text> {
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String text = value.toString(); //�������Text���͵���ĸ��valueתΪString����
			
			char[] textCharArray = text.toCharArray(); //��String���͵���ĸ��ת���ַ�����
			
			Arrays.sort(textCharArray); //���ַ������������
			
			String sortedText = new String(textCharArray); //���������ַ����飬ת��String�ַ���
			
			context.write(new Text(sortedText), value);  //д��context,���key����������ĸ�������value��ԭʼ��ĸ��
			
		}
		
	}
	
	//дReduce����
	public static class Anagramsreducer extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			StringBuffer res = new StringBuffer(); //�½�һ���յ�StringBufferʵ��res
			
			int count = 0;  //��������ʼֵΪ0
			
			//��ʼ����values���ֵ
			for (Text text : values) {
				
				//���res����������ֵ�������ֵ��ʱ���ȼӵ�һ�������������ָ��
				if(res.length() > 0) {
					res.append(",");
				}
				
				//��res�����values���ֵ
				res.append(text);
				
				//����
				count++;
			}
			
			//������������������������ͬ��ĸ��ɵĵ��ʣ�����ʾ
			if(count > 1) {
				context.write(key, new Text(res.toString()));
			}
			
			
		}
		
	}
	
	
	

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		String[] arg0 = {"hdfs://dsj:9000/anagrams/",
				"hdfs://dsj:9000/anagrams/out"};
		//ִ��mapreduce
		int ec = ToolRunner.run(new Configuration(), new Anagrams(), arg0);
		
		//�����˳�
		
		System.exit(ec);
		

	}



	
    //дRun����    
	@Override
	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
		
		//��������
		Configuration conf = new Configuration();
		
		//���Ŀ¼��������ھ�ɾ��
		Path mypath = new Path(arg0[1]);
		
		FileSystem fs =mypath.getFileSystem(conf);
		
		if(fs.isDirectory(mypath)) {
			fs.delete(mypath, true);
		}
		
		//����Job����
		Job job = new Job(conf,"Anagrams");
		
		job.setJarByClass(Anagrams.class);
		
		//ָ�����롢���Ŀ¼
		
		FileInputFormat.addInputPath(job, new Path(arg0[0]));
		FileOutputFormat.setOutputPath(job, new Path(arg0[1]));
		
		//ָ��Mapper��Reduce
		
		job.setMapperClass(Anagramsmapper.class);
		job.setReducerClass(Anagramsreducer.class);
		
		//ָ��Mapper��Reduce���������
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		//�ύ��ҵ
		
		return job.waitForCompletion(true) ? 0: 1;
	}

}
