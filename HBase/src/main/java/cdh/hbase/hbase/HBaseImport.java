package cdh.hbase.hbase;



import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;


public class HBaseImport {
static class BatchMapper extends Mapper<LongWritable, Text, LongWritable, Text>{
@Override
protected void map(LongWritable key, Text value,
Mapper<LongWritable, Text, LongWritable, Text>.Context context)
throws IOException, InterruptedException {
String line = value.toString();
String[] splited = line.split("\t");
SimpleDateFormat simpleDateFormatimpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
String format = simpleDateFormatimpleDateFormat.format(new Date(Long.parseLong(splited[0].trim())));
String rowKey=splited[1]+"_"+format;
Text v2s = new Text();
v2s.set(rowKey+"\t"+line);
context.write(key, v2s);
}
}
static class BatchReducer extends TableReducer<LongWritable, Text, NullWritable>{
private String family="cf";//列族


@Override
protected void reduce(LongWritable arg0, Iterable<Text> v2s,
Reducer<LongWritable, Text, NullWritable, Mutation>.Context context)
throws IOException, InterruptedException {
for (Text v2 : v2s) {
String[] splited = v2.toString().split("\t");
String rowKey = splited[0];
Put put = new Put(rowKey.getBytes());
put.add(family.getBytes(), "raw".getBytes(), v2.toString().getBytes());
put.add(family.getBytes(), "rePortTime".getBytes(), splited[1].getBytes());
put.add(family.getBytes(), "msisdn".getBytes(), splited[2].getBytes());
put.add(family.getBytes(), "apmac".getBytes(), splited[3].getBytes());
put.add(family.getBytes(), "acmac".getBytes(), splited[4].getBytes());
put.add(family.getBytes(), "host".getBytes(), splited[5].getBytes());
put.add(family.getBytes(), "siteType".getBytes(), splited[6].getBytes());
put.add(family.getBytes(), "upPackNum".getBytes(), splited[7].getBytes());
put.add(family.getBytes(), "downPackNum".getBytes(), splited[8].getBytes());
put.add(family.getBytes(), "upPayLoad".getBytes(), splited[9].getBytes());
put.add(family.getBytes(), "downPayLoad".getBytes(), splited[10].getBytes());
put.add(family.getBytes(), "httpStatus".getBytes(), splited[11].getBytes());
context.write(NullWritable.get(), put);
}
}
}
private static final String TableName = "waln_log";
public static void main(String[] args) throws Exception {
Configuration conf = HBaseConfiguration.create();
conf.set("hbase.zookeeper.quorum","192.168.80.20,192.168.80.21,192.168.80.22");
//conf.set("hbase.rootdir", "hdfs://cluster/hbase");
conf.set("hbase.rootdir", "hdfs://192.168.80.20:9000/hbase");
conf.set(TableOutputFormat.OUTPUT_TABLE, TableName);

Job job = new Job(conf, HBaseImport.class.getSimpleName());
TableMapReduceUtil.addDependencyJars(job);
job.setJarByClass(HBaseImport.class);

job.setMapperClass(BatchMapper.class);
job.setReducerClass(BatchReducer.class);

job.setMapOutputKeyClass(LongWritable.class);
job.setMapOutputValueClass(Text.class);

job.setInputFormatClass(TextInputFormat.class);
job.setOutputFormatClass(TableOutputFormat.class);

FileInputFormat.setInputPaths(job, "hdfs://192.168.80.20:9000/data");
System.out.println("xxxxxxx1xxxxxxxx");
job.waitForCompletion(true);
}
}


