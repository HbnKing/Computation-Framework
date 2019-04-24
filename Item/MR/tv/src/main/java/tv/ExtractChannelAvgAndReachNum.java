package tv;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
/**
 * 
 * 针对上一步的结果统计每个频道每分钟的平均收视人数和平均到达人数
 * 
 */
public class ExtractChannelAvgAndReachNum  extends Configured implements Tool {
	public static class ExtractChannelAvgAndReachNumMapper extends
			Mapper<LongWritable, Text, Text, Text> {
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// stbNum0+"@"+date1+"@"+sn2+"@"+p3+"@"+s4+"@"+e5+"@"+duration6
			String[] kv = StringUtils.split(value.toString(), "@");
			// 过滤掉不合格数据
			if (kv.length != 7) {
				return;
			}
			// 机顶盒号
			String stbnum = kv[0].trim();
			// 日期
			String date = kv[1].trim();
			// 频道
			String channel = kv[2].trim();
			// 起始时间
			int start = TimeUtil.TimeToSecond(kv[4].trim());
			// 结束时间
			int end = TimeUtil.TimeToSecond(kv[5].trim());

			// 将时间段解析为每分钟记录，比如23:51:45~23:56:45之间的每分钟
			List<String> list = TimeUtil.getTimeSplit(kv[4], kv[5]);
			int size = list.size();
			// 循环统计所有指标每分钟的数据
			for (int i = 0; i < size; i++) {
				// 根据start end 切割的每分钟
				String min = list.get(i);

				// 输出每分钟栏目的收视人数（1）和到达人数（0，1）
				if ((end - start) > 60) {
					// 达到人数满足此条件
					context.write(new Text(channel + "@" + date + "@" + min),
							new Text(stbnum + "@" + stbnum));
				}
				context.write(new Text(channel + "@" + date + "@" + min),
						new Text(stbnum + "@"));
			}
		}
	}

	public static class ExtractChannelAvgAndReachNumReduce extends
			Reducer<Text, Text, Text, Text> {
		private Text result = new Text();
		// 定义平均收视人数集合
		private Set<String> set_avgnum = new HashSet<String>();
		// 定义平均到达人数集合
		private Set<String> set_reachnum = new HashSet<String>();

		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			set_avgnum.clear();
			set_reachnum.clear();
			for (Text value : values) {
				String[] arr = StringUtils.split(value.toString(), "@");
				set_avgnum.add(arr[0]);
				// 满足到达条件
				if (arr.length > 1) {
					set_reachnum.add(arr[1]);
				}
			}
			// 计算出每分钟的收视人数和到达人数
			result.set(set_avgnum.size() + "@" + set_reachnum.size());
			context.write(key, result);
		}

	}

	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err
					.println("Usage: ExtractChannelAvgAndReachNum [<in>...] <out>");
			System.exit(2);
		}

		Job job = Job.getInstance();

		// 设置输出key value分隔符
		job.getConfiguration().set(
				"mapreduce.output.textoutputformat.separator", "@");

		job.setJarByClass(ExtractChannelAvgAndReachNum.class);
		job.setMapperClass(ExtractChannelAvgAndReachNumMapper.class);
		job.setReducerClass(ExtractChannelAvgAndReachNumReduce.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// 设置输入路径
		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}

		// 设置输出路径
		FileOutputFormat.setOutputPath(job, new Path(
				otherArgs[otherArgs.length - 1]));
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int ec = ToolRunner.run(new Configuration(),
				new ExtractChannelAvgAndReachNum(), args);
		System.exit(ec);
	}
}
