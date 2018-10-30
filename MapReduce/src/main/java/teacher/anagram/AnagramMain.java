package teacher.anagram;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
public class AnagramMain extends Configured implements Tool{
    
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
        job.setJarByClass(AnagramMain. class);	//设置主类
        
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
        String[] args0 = { "hdfs://wang:9000/anagram/anagram.txt" ,
        "hdfs://wang:9000/anagram/output"};
        int ec = ToolRunner.run( new Configuration(), new AnagramMain(), args0);
        System. exit(ec);
    }
}